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
  describe fname $ do
    it "write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-write")
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
    it "relative path" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-relative-path")
            testWritingInRelativeDir fp = do
              writeFileUtf8Builder fp $ displayBytesUtf8 hello
              withFileTestable fp ReadWriteMode $ \h -> do
                input <- BS.hGetLine h
                input `shouldBe` hello
                BS.hPut h goodbye
              BS.readFile fp `shouldReturn` (hello <> goodbye)
        bracket
          (createDirectoryIfMissing True dir >> pure (dir </> "test.file"))
          (const (removeDirectoryRecursive dir))
          testWritingInRelativeDir



writeBinaryFileSpec :: String -> (FilePath -> ByteString -> IO a) -> SpecWith ()
writeBinaryFileSpec fname writeFileTestable = do
  let hello = "Hello World"
  describe fname $ do
    it "write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> (fname ++ "-write")
        writeFileTestable fp hello
        BS.readFile fp `shouldReturn` hello
    xit "default-permissions" $ () `shouldBe` ()
