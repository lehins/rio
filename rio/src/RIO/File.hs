{-# LANGUAGE CPP                      #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE NoImplicitPrelude        #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ViewPatterns             #-}
{-|

== Rationale

This module offers functions to handle files that offer better durability and/or
atomicity.

== When to use the functions on this module?

Given the usage of this functions comes at a cost in performance, it is important
to consider what are the use cases that are ideal for each of the functions.

=== Not Durable and not Atomic

For this use case, you want to use the regular functions:

* 'System.IO.withBinaryFile'
* 'RIO.writeFileBinary'

The regular use case for this scenario happens when your program is dealing with
outputs that are never going to be consumed again by your program. For example,
imagine you have a program that generates sales reports for the last month, this
is a report that can be generated quickly; you don't really care if the output
file gets corrupted or lost at one particular execution of your program given
that is cheap to execute the data export program a second time. In other words,
your program doesn't /rely/ on the data contained in this file in order to work.

=== Atomic but not Durable

 Imagine a scenario where your program builds a temporary file that serves as an
intermediate step to a bigger task, like Object files (@.o@) in a compilation
process. The program will use an existing @.o@ file if it is present, or it will
build one from scratch if it is not. The file is not really required, but if it
is present, it *must* be valid and consistent. In this situation, you care about
atomicity, but not durability.

There is no function exported by this module that provides /only/ atomicity.

=== Durable but not Atomic

For this use case, you want to use the functions:

* 'withBinaryFileDurable'
* 'writeBinaryFileDurable'

The regular use case for this scenario happens when your program deals with file
modifications that must be guaranteed to be durable, but you don't care that
changes are consistent. If you use this function, more than likely your program
is ensuring consistency guarantees through other means, for example, SQLite uses
 the Write Ahead Log (WAL) algorithm to ensure changes are atomic at an
application level.

=== Durable and Atomic

For this use case, you can use the functions:

* 'withBinaryFileDurableAtomic'
* 'writeBinaryFileDurableAtomic'

The regular use case for this scenario happens when you want to ensure that
after a program is executed, the modifications done to a file are guaranteed to
be saved, and also that changes are rolled-back in case there is a failure (e.g.
hard reboot, shutdown, etc).

@since 0.1.6
-}
module RIO.File
  (
    writeBinaryFileDurable
  , writeBinaryFileDurableAtomic
  , writeBinaryFileAtomic
  , withBinaryFileDurable
  , withBinaryFileDurableAtomic
  , withBinaryFileAtomic
  , ensureFileDurable
  )
  where

import           RIO.Prelude.Reexports

#ifdef WINDOWS
import           RIO.Prelude.IO

#else

import           RIO.Directory          (doesFileExist)
import           RIO.ByteString         (hPut)
import           Data.Bits              ((.|.))
import           Data.Typeable          (cast)
import           Foreign                (allocaBytes)
import           Foreign.C              (CInt (..), throwErrnoIfMinus1,
                                         throwErrnoIfMinus1Retry)
import           GHC.IO.Device          (IODeviceType (RegularFile))
import qualified GHC.IO.Device          as Device
import qualified GHC.IO.FD              as FD
import qualified GHC.IO.Handle.FD       as HandleFD
import           System.Directory       (copyFile, removeFile)
import           System.FilePath        (isRelative, takeDirectory, takeFileName, (</>))
import qualified System.Posix.Files     as Posix
import           System.Posix.Internals (CFilePath, c_close, c_safe_open,
                                         withFilePath)
import           System.Posix.Types     (FileMode, CMode (..), Fd (..))
import           System.IO              (openBinaryTempFile, hPutBuf, hGetBuf, SeekMode(..))
import           System.IO.Error        (isDoesNotExistError)

#if MIN_VERSION_base(4,9,0)
import qualified GHC.IO.Handle.Types    as HandleFD (Handle (..), Handle__ (..))
#endif

import           Debug.Trace

-- TODO: Add a ticket/pull request to export this symbols from
-- System.Internal.Posix
--
-- NOTE: System.Posix.Internal doesn't re-export this constants so we have to
-- recreate-them here
foreign import ccall unsafe "HsBase.h __hscore_o_rdonly" o_RDONLY :: CInt
foreign import ccall unsafe "HsBase.h __hscore_o_wronly" o_WRONLY :: CInt
foreign import ccall unsafe "HsBase.h __hscore_o_rdwr"   o_RDWR   :: CInt
foreign import ccall unsafe "HsBase.h __hscore_o_append" o_APPEND :: CInt
foreign import ccall unsafe "HsBase.h __hscore_o_creat"  o_CREAT  :: CInt
foreign import ccall unsafe "HsBase.h __hscore_o_noctty" o_NOCTTY :: CInt

-- After here, we have our own imports
foreign import ccall safe "fcntl.h openat"
  c_safe_openat :: Fd -> CFilePath -> CInt -> CMode -> IO CInt

foreign import ccall safe "fcntl.h renameat"
  c_safe_renameat :: Fd -> CFilePath -> Fd -> CFilePath -> IO CInt

foreign import ccall safe "unistd.h fsync"
  c_safe_fsync :: Fd -> IO CInt

std_flags, output_flags, read_flags, write_flags, rw_flags,
    append_flags :: CInt
std_flags    = o_NOCTTY
output_flags = std_flags    .|. o_CREAT
read_flags   = std_flags    .|. o_RDONLY
write_flags  = output_flags .|. o_WRONLY
rw_flags     = output_flags .|. o_RDWR
append_flags = write_flags  .|. o_APPEND

ioModeToFlags :: IOMode -> CInt
ioModeToFlags iomode =
  case iomode of
    ReadMode      -> read_flags
    WriteMode     -> write_flags
    ReadWriteMode -> rw_flags
    AppendMode    -> append_flags

-- | Returns a low-level file descriptor for a directory path. This function
-- exists given the fact that 'openFile' does not work with directories.
--
-- If you use this function, make sure you are working on a masked state,
-- otherwise async exceptions may leave file descriptors open.
--
-- @since 0.1.6
openDir :: MonadIO m => FilePath -> m Fd
openDir fp
  -- TODO: Investigate what is the situation with Windows FS in regards to non_blocking
  -- NOTE: File operations _do not support_ non_blocking on various kernels, more
  -- info can be found here: https://ghc.haskell.org/trac/ghc/ticket/15153
 =
  liftIO $
  withFilePath fp $ \cFp ->
    Fd <$>
    (throwErrnoIfMinus1Retry "openDir" $
     c_safe_open cFp (ioModeToFlags ReadMode) 0o660)

-- | Closes a 'Fd' that points to a Directory.
--
-- @since 0.1.6
closeDirectory :: MonadIO m => Fd -> m ()
closeDirectory (Fd dirFd) =
  liftIO $
  void $
  throwErrnoIfMinus1Retry "closeDirectory" $ c_close dirFd

-- | Executes the low-level C function fsync on a C file descriptor
--
-- @since 0.1.6
fsyncFileDescriptor
  :: MonadIO m
  => String -- ^ Meta-description for error messages
  -> Fd   -- ^ C File Descriptor
  -> m ()
fsyncFileDescriptor name fd =
  liftIO $
  void $
    throwErrnoIfMinus1 ("fsync - " <> name) $
    c_safe_fsync fd

-- | Opens a file from a directory, using this function in favour of a regular
-- 'openFile' guarantees that any file modifications are kept in the same
-- directory where the file was opened. An edge case scenario is a mount
-- happening in the directory where the file was opened while your program is
-- running.
--
-- If you use this function, make sure you are working on an masked state,
-- otherwise async exceptions may leave file descriptors open.
--
openFileFromDir :: MonadIO m => Fd -> FilePath -> IOMode -> m Handle
openFileFromDir dirFd (dropDirectoryIfRelative -> fp) iomode =
  liftIO $
  withFilePath fp $ \f ->
    bracketOnError
      (do fileFd <- throwErrnoIfMinus1Retry "openFileFromDir" $
                      c_safe_openat dirFd f (ioModeToFlags iomode)
                                            0o666 {- Can open directory with read only -}
          FD.mkFD
             fileFd
             iomode
             Nothing {- no stat -}
             False {- not a socket -}
             False {- non_blocking -}
            `onException` c_close fileFd)
      (liftIO . Device.close . fst)
      (\(fD, fd_type) -> do
         -- we want to truncate() if this is an open in WriteMode, but only if the
         -- target is a RegularFile. ftruncate() fails on special files like
         -- /dev/null.
         when (iomode == WriteMode && fd_type == RegularFile) $
           Device.setSize fD 0
         HandleFD.mkHandleFromFD fD fd_type fp iomode False Nothing)


-- | Opens a file using the openat C low-level API. This approach allows us to
-- get a file descriptor for the directory that contains the file, which we can
-- use later on to fsync the directory with.
--
-- If you use this function, make sure you are working on an masked state,
-- otherwise async exceptions may leave file descriptors open.
--
-- @since 0.1.6
openFileAndDirectory :: MonadIO m => FilePath -> IOMode -> m (Fd, Handle)
openFileAndDirectory filePath iomode =  liftIO $ do
  let dir = takeDirectory filePath
      fp = takeFileName filePath

  bracketOnError (openDir dir) closeDirectory $ \dirFd -> do
    fileHandle <- openFileFromDir dirFd fp iomode
    return (dirFd, fileHandle)

withDirectory :: MonadUnliftIO m => FilePath -> (Fd -> m a) -> m a
withDirectory dirPath = bracketOnError (openDir dirPath) closeDirectory

withFileInDirectory dirFd filePath iomode =
  bracket (openFileFromDir dirFd filePath iomode) hClose

-- | This sub-routine does the following tasks:
--
-- * It calls fsync and then closes the given Handle (mapping to a temporary/backup filepath)
-- * It calls fsync and then closes the containing directory of the file
--
-- These steps guarantee that the file changes are durable.
--
-- @since 0.1.6
closeFileDurable :: MonadIO m => Fd -> Handle -> m ()
closeFileDurable dirFd hdl =
  liftIO $
  finally
    (do fsyncFileHandle "closeFileDurable" Nothing hdl
        -- NOTE: Here we are purposefully not fsyncing the directory if the file fails to fsync
        fsyncDirectoryFd "closeFileDurable" dirFd)
    (closeDirectory dirFd)

-- | Optionally set file permissions, call @fsync@ on the file handle and then
-- close it.
fsyncFileHandle :: String -> Maybe FileMode -> Handle -> IO ()
fsyncFileHandle fname mFileMode hdl = withHandleFd hdl fsyncFd `finally` hClose hdl
  where fsyncFd fd = do
          forM_ mFileMode (Posix.setFdMode fd)
          fsyncFileDescriptor (fname ++ "/File") fd

-- | Call @fsync@ on the opened directory file descriptor
fsyncDirectoryFd :: String -> Fd -> IO ()
fsyncDirectoryFd fname = fsyncFileDescriptor (fname ++ "/Directory")

buildTemporaryFilePath :: MonadIO m => FilePath -> m FilePath
buildTemporaryFilePath filePath = liftIO $ do
  let
    dirFp  = takeDirectory filePath
    fileFp = takeFileName filePath
  bracket (openBinaryTempFile dirFp fileFp)
          (hClose . snd)
          (return . fst)

toTmpFilePath :: MonadIO m => FilePath -> m FilePath
toTmpFilePath filePath =
    buildTemporaryFilePath (dirPath </> tmpFilename)
  where
    dirPath = takeDirectory filePath
    filename = takeFileName filePath
    tmpFilename = "." <> filename <> ".tmp"


-- | Create a temporary file for a matching possibly exiting target file that
-- will be replaced in the future. Temporary file
-- is meant to be renamed afterwards, thus it is only delted upon error.
--
-- __Important__: Temporary file is not removed and file handle is not closed if
-- there was no exception thrown by the supplied action.
withBinaryTempFileFor ::
     MonadUnliftIO m
  => FilePath
  -- ^ Source file path. It may exist or may not.
  -> IOMode
  -> (FilePath -> Handle -> m a)
  -> m a
withBinaryTempFileFor filePath iomode action =
  bracketOnError
    (liftIO (openBinaryTempFile dirPath tmpFileName))
    (\(tmpFilePath, tmpFileHandle) ->
       liftIO (hClose tmpFileHandle >> tryIO (removeFile tmpFileName)))
    (uncurry action)
  where
    dirPath = takeDirectory filePath
    fileName = takeFileName filePath
    tmpFileName = "." <> fileName <> ".tmp"


-- | Copy the contents of the file into the handle, but only if that file exists
-- and either `ReadWriteMode` or `AppendMode` is specified. Returned is the file
-- mode of the original file so it can be set later on the copy.
copyFileHandle ::
     MonadUnliftIO f => IOMode -> FilePath -> Handle -> f (Maybe FileMode)
copyFileHandle iomode fromFilePath toHandle =
  if iomode == ReadWriteMode || iomode == AppendMode
    -- Make a copy of the source file and its permissions, but only if it exists
    then either (const Nothing) Just <$>
         tryJust (guard . isDoesNotExistError)
           (do fileStatus <- liftIO $ Posix.getFileStatus fromFilePath
               withBinaryFile fromFilePath ReadMode (`copyHandleData` toHandle)
               -- rewind to the beginning
               hSeek toHandle AbsoluteSeek 0
               pure $ Posix.fileMode fileStatus)
    else pure Nothing


-- This is a copy of the internal function from `directory-1.3.3.2`. It became
-- available only in directory-1.3.3.0 and is still internal, hence the
-- duplication.
copyHandleData :: MonadIO m => Handle -> Handle -> m ()
copyHandleData hFrom hTo = liftIO $ allocaBytes bufferSize go
  where
    bufferSize = 131072 -- 128 KiB, as coreutils `cp` uses as of May 2014 (see ioblksize.h)
    go buffer = do
      count <- hGetBuf hFrom buffer bufferSize
      when (count > 0) $ do
        hPutBuf hTo buffer count
        go buffer


withHandleFd :: Handle -> (Fd -> IO a) -> IO a
withHandleFd h cb =
  case h of
    HandleFD.FileHandle _ mv ->
      withMVar mv $ \HandleFD.Handle__{HandleFD.haDevice = dev} ->
        case cast dev of
          Just fd -> cb $ Fd $ FD.fdFD fd
          Nothing -> error "withHandleFd: not a file handle"
    HandleFD.DuplexHandle {} -> error "withHandleFd: not a file handle"


-- | This sub-routine does the following tasks:
--
-- * It calls fsync and then closes the given Handle (mapping to a temporary/backup filepath)
-- * It renames the file to the original path (using renameat)
-- * It calls fsync and then closes the containing directory of the file
--
-- These steps guarantee that the file is durable, and that the backup mechanism
-- for catastrophic failure is discarded after no error is thrown.
renameFileAtomic ::
     MonadIO m =>
       FilePath
     -- ^ Temporary file path for the file to be renamed. Can be relative.
     -> FilePath
     -- ^ File path for the target where the temporary file will be renamed
     -- to. Can be relative.
     -> Fd
     -- ^ File descriptor for the directory where both the original temporary
     -- file and the target files are located. In other words atomic rename will
     -- fail if rename happens across different parent directories
     -> m ()
renameFileAtomic tmpFilePath filePath dirFd =
  liftIO $
      withFilePath (dropDirectoryIfRelative tmpFilePath) $ \tmpFp ->
         withFilePath (dropDirectoryIfRelative filePath) (renameFile tmpFp)
  where
    renameFile tmpFp origFp =
      void $
      throwErrnoIfMinus1Retry "renameFileAtomicWithCallbacks - renameFile" $
      c_safe_renameat dirFd tmpFp dirFd origFp

-- If the `filePath` given is relative, then it is interpreted relative to the directory
-- referred to by the file descriptor cDirFd (rather than relative to the current working
-- directory). See `man renameat` for more info.
dropDirectoryIfRelative :: FilePath -> FilePath
dropDirectoryIfRelative fp
  | isRelative fp = takeFileName fp
  | otherwise = fp


withBinaryFileDurableAtomicPosix ::
     MonadUnliftIO m => FilePath -> IOMode -> (Handle -> m r) -> m r
withBinaryFileDurableAtomicPosix filePath iomode action =
  case iomode of
    ReadMode
      -- We do not need to consider an atomic operation when we are in a
      -- 'ReadMode', so we can use a regular `withBinaryFile`
     -> withBinaryFile filePath iomode action
    _ {- WriteMode,  ReadWriteMode,  AppendMode -}
     ->
      withBinaryTempFileFor filePath iomode $ \tmpFilePath tmpFileHandle' -> do
        hClose tmpFileHandle'
        -- We need to close the newly created file, since it must be opened at
        -- the directory for durability guarantees
        withDirectory (takeDirectory filePath) $ \dirFd ->
          withFileInDirectory dirFd tmpFilePath iomode $ \tmpFileHandle -> do
            mFileMode <- copyFileHandle iomode filePath tmpFileHandle
            res <- action tmpFileHandle
            liftIO $
              fsyncFileHandle
                "withBinaryFileDurableAtomicPosix"
                mFileMode
                tmpFileHandle
            renameFileAtomic tmpFilePath filePath dirFd
            liftIO $ fsyncDirectoryFd "withBinaryFileDurableAtomic" dirFd
            pure res

withBinaryFileAtomicPosix ::
     MonadUnliftIO m => FilePath -> IOMode -> (Handle -> m r) -> m r
withBinaryFileAtomicPosix filePath iomode action =
  case iomode of
    ReadMode
      -- We do not need to consider an atomic operation when we are in a
      -- 'ReadMode', so we can use a regular `withBinaryFile`
     -> withBinaryFile filePath iomode action
    _ {- WriteMode,  ReadWriteMode,  AppendMode -}
     ->
      withBinaryTempFileFor filePath iomode $ \tmpFilePath tmpFileHandle -> do
        res <- action tmpFileHandle
        liftIO $ Posix.rename tmpFilePath filePath
        pure res

#endif

-- | After a file is closed, it opens it again and executes fsync internally on
-- both the file and the directory that contains it. Note this function is
-- intended to work around the non-durability of existing file APIs, as opposed
-- to being necessary for the API functions provided in 'RIO.File' module.
--
-- [The effectiveness of calling this function is
-- debatable](https://stackoverflow.com/questions/37288453/calling-fsync2-after-close2/50158433#50158433),
-- as it relies on internal implementation details at the Kernel level that
-- might change. We argue that, despite this fact, calling this function may
-- bring benefits in terms of durability.
--
-- === Cross-Platform support
--
-- This function is a noop on Windows platforms.
--
-- @since 0.1.6
ensureFileDurable :: MonadIO m => FilePath -> m ()
ensureFileDurable fp =
#if WINDOWS
  fp `seq` return ()
#else
  liftIO $
  bracket (openFileAndDirectory fp ReadMode)
          (uncurry closeFileDurable)
          (const $ return ())
#endif


-- | Similar to 'writeFileBinary', but it also ensures that changes executed to
-- the file are guaranteed to be durable. It internally uses fsync and makes
-- sure it synchronizes the file on disk.
--
-- === Cross-Platform support
--
-- This function behaves the same as 'RIO.writeFileBinary' on Windows platforms.
--
-- @since 0.1.6
writeBinaryFileDurable :: MonadIO m => FilePath -> ByteString -> m ()
writeBinaryFileDurable fp bytes =
#if WINDOWS
  liftIO $ writeFileBinary fp bytes
#else
  liftIO $ withBinaryFileDurable fp WriteMode (liftIO . (`hPut` bytes))
#endif

-- | Similar to 'writeFileBinary', but it also guarantes that changes executed
-- to the file are durable, also, in case of failure, the modified file is never
-- going to get corrupted. It internally uses fsync and makes sure it
-- synchronizes the file on disk.
--
-- === Cross-Platform support
--
-- This function behaves the same as 'RIO.writeFileBinary' on Windows platforms.
--
-- @since 0.1.6
writeBinaryFileDurableAtomic :: MonadIO m => FilePath -> ByteString -> m ()
writeBinaryFileDurableAtomic fp bytes =
#if WINDOWS
  liftIO $ writeFileBinary fp bytes
#else
  liftIO $ withBinaryFileDurableAtomic fp WriteMode (liftIO . (`hPut` bytes))
#endif

-- | Same as 'writeBinaryFileDurableAtomic', except it does not guarantee durability.
--
-- === Cross-Platform support
--
-- This function behaves the same as 'RIO.writeFileBinary' on Windows platforms.
--
-- @since 0.1.10
writeBinaryFileAtomic :: MonadIO m => FilePath -> ByteString -> m ()
writeBinaryFileAtomic fp bytes =
#if WINDOWS
  liftIO $ writeFileBinary fp bytes
#else
  liftIO $ withBinaryFileAtomic fp WriteMode (liftIO . (`hPut` bytes))
#endif

-- | Opens a file with the following guarantees:
--
-- * It successfully closes the file in case of an asynchronous exception
--
-- * It reliably saves the file in the correct directory; including edge case
--   situations like a different device being mounted to the current directory,
--   or the current directory being renamed to some other name while the file is
--   being used.
--
-- * It ensures durability by executing an fsync call before closing the file
--   handle
--
-- === Cross-Platform support
--
-- This function behaves the same as 'System.IO.withBinaryFile' on Windows platforms.
--
-- @since 0.1.6
withBinaryFileDurable ::
     MonadUnliftIO m => FilePath -> IOMode -> (Handle -> m r) -> m r
withBinaryFileDurable fp iomode cb =
#if WINDOWS
  withBinaryFile fp iomode cb
#else
  withRunInIO $ \run ->
    bracket
      (openFileAndDirectory fp iomode)
      (uncurry closeFileDurable)
      (run . cb . snd)
#endif

-- | Opens a file with the following guarantees:
--
-- * It successfully closes the file in case of an asynchronous exception
--
-- * It reliably saves the file in the correct directory; including edge case
--   situations like a different device being mounted to the current directory,
--   or the current directory being renamed to some other name while the file is
--   being used.
--
-- * It ensures durability by executing an fsync call before closing the file
--   handle
--
-- * It keeps all changes in a temporary file, and after it is closed it atomically
--   moves the temporary file to the original filepath, in case of catastrophic
--   failure, the original file stays unaffected.
--
-- __Important__ - Make sure not to close the `Handle`, otherwise durability
-- might not work. It will be closed after the supplied action will finish.
--
-- === Performance Considerations
--
-- When using a writable but non-truncating 'IOMode' (i.e. 'ReadWriteMode' and
-- 'AppendMode'), this function performs a copy operation of the specified input
-- file to guarantee the original file is intact in case of a catastrophic
-- failure (no partial writes). This approach may be prohibitive in scenarios
-- where the input file is expected to be large in size.
--
-- === Cross-Platform support
--
-- This function behaves the same as 'System.IO.withBinaryFile' on Windows
-- platforms.
--
-- @since 0.1.6
withBinaryFileDurableAtomic ::
     MonadUnliftIO m => FilePath -> IOMode -> (Handle -> m r) -> m r
withBinaryFileDurableAtomic =
#if WINDOWS
  withBinaryFile
#else
  withBinaryFileDurableAtomicPosix
#endif

-- | Just like `withBinaryFileDurableAtomic`, but without the durability
-- part. What it means is that the file can still disappear after it has been
-- succesfully written due to some extreme event like an abrupt power loss, but
-- the contents will not be compromised in case when the file write did not end
-- successfully.
--
-- @since 0.1.10
withBinaryFileAtomic ::
     MonadUnliftIO m => FilePath -> IOMode -> (Handle -> m r) -> m r
withBinaryFileAtomic =
#if WINDOWS
  withBinaryFile
#else
  withBinaryFileAtomicPosix
#endif
