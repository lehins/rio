{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
module RIO.FileSpec where

import Test.Hspec
import System.FilePath ((</>))
import UnliftIO.Temporary (withSystemTempDirectory)
import UnliftIO.Directory

import RIO
import qualified RIO.ByteString as BS
import qualified RIO.File as File

spec :: Spec
spec = do
  describe "ensureFileDurable" $
    it "ensures a file is durable with an fsync" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> "ensure_file_durable"
        writeFileUtf8 fp "Hello World"
        File.ensureFileDurable fp
        contents <- BS.readFile fp
        contents `shouldBe` "Hello World"
  withBinaryFileSpec "withBinaryFileAtomic" File.withBinaryFileAtomic
  writeBinaryFileSpec "writeBinaryFileAtomic" File.writeBinaryFileAtomic
  withBinaryFileSpec "withBinaryFileDurableAtomic" File.withBinaryFileDurableAtomic
  writeBinaryFileSpec "writeBinaryFileDurableAtomic" File.writeBinaryFileDurableAtomic

withBinaryFileSpec ::
     String -> (FilePath -> IOMode -> (Handle -> IO ()) -> IO a) -> Spec
withBinaryFileSpec fname withFileTestable = do
  let hello = "Hello World"
      goodbye = "Goodbye World"
      modifiedPermissions =
        setOwnerExecutable True $
        setOwnerReadable True $ setOwnerWritable True emptyPermissions
  describe fname $ do
    it "write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-write")
        writeFileUtf8Builder fp $ displayBytesUtf8 goodbye
        withFileTestable fp WriteMode $ \h -> BS.hPut h hello
        BS.readFile fp `shouldReturn` hello
    it "read/write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-read-write")
        writeFileUtf8Builder fp $ displayBytesUtf8 hello
        withFileTestable fp ReadWriteMode $ \h -> do
          BS.hGetLine h `shouldReturn` hello
          BS.hPut h goodbye
        BS.readFile fp `shouldReturn` (hello <> goodbye)
    it "append" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-append")
            privet = "Привет Мир" -- some unicode won't hurt
        writeFileUtf8Builder fp $ display privet
        setPermissions fp modifiedPermissions
        withFileTestable fp AppendMode $ \h -> BS.hPut h goodbye
        BS.readFile fp `shouldReturn` (encodeUtf8 privet <> goodbye)
    it "sub-directory" $
      withSystemTempDirectory "rio" $ \dir -> do
        let subDir = dir </> (fname ++ "-sub-directory")
        let testWritingInRelativeDir fp = do
              writeFileUtf8Builder fp $ displayBytesUtf8 hello
              withFileTestable fp ReadWriteMode $ \h -> do
                input <- BS.hGetLine h
                input `shouldBe` hello
                BS.hPut h goodbye
              BS.readFile fp `shouldReturn` (hello <> goodbye)
        bracket
          (createDirectoryIfMissing True subDir >> pure (subDir </> "test.file"))
          (const (removeDirectoryRecursive dir))
          testWritingInRelativeDir
    it "relative-directory" $
      withSystemTempDirectory "rio" $ \dir -> do
        let relDir = fname ++ "-relative-directory"
            subDir = dir </> relDir
        let testWritingInRelativeDir fp =
              withCurrentDirectory dir $ do
                writeFileUtf8Builder fp $ displayBytesUtf8 hello
                withFileTestable fp ReadWriteMode $ \h -> do
                  input <- BS.hGetLine h
                  input `shouldBe` hello
                  BS.hPut h goodbye
                BS.readFile fp `shouldReturn` (hello <> goodbye)
        bracket
          (createDirectoryIfMissing True subDir >> pure (relDir </> "test.file"))
          (const (removeDirectoryRecursive dir))
          testWritingInRelativeDir
    it "modified-permissions" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-modified-permissions")
        writeFileUtf8Builder fp $ displayBytesUtf8 hello
        setPermissions fp modifiedPermissions
        withFileTestable fp AppendMode $ \h -> BS.hPut h goodbye
        getPermissions fp `shouldReturn` modifiedPermissions



writeBinaryFileSpec :: String -> (FilePath -> ByteString -> IO a) -> SpecWith ()
writeBinaryFileSpec fname writeFileTestable = do
  let hello = "Hello World"
      defaultPermissions =
        setOwnerReadable True $ setOwnerWritable True emptyPermissions
  describe fname $ do
    it "write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-write")
        writeFileTestable fp hello
        BS.readFile fp `shouldReturn` hello
    it "default-permissions" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-default-permissions")
        writeFileTestable fp hello
        getPermissions fp `shouldReturn` defaultPermissions
