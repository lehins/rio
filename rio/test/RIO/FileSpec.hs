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
import qualified RIO.File as SUT

spec :: Spec
spec = do
  describe "ensureFileDurable" $
    it "ensures a file is durable with an fsync" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> "ensure_file_durable"
        writeFileUtf8 fp "Hello World"
        SUT.ensureFileDurable fp
        contents <- BS.readFile fp
        contents `shouldBe` "Hello World"

  describe "withBinaryFileDurableAtomic" $ do
    it "read/write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> "with_file_durable_atomic"
            hello = "Hello World"
            goodbye = "Goodbye World"
        writeFileUtf8Builder fp $ displayBytesUtf8 hello
        SUT.withBinaryFileDurableAtomic fp ReadWriteMode $ \h -> do
          input <- BS.hGetLine h
          input `shouldBe` hello
          BS.hPut h goodbye
        BS.readFile fp `shouldReturn` (hello <> goodbye)
    it "relative path" $
      withSystemTempDirectory "rio" $ \dir -> do
        let dir = "with_file_durable_atomic"
            hello = "Hello World"
            goodbye = "Goodbye World"
            testWritingInRelativeDir fp = do
              writeFileUtf8Builder fp $ displayBytesUtf8 hello
              SUT.withBinaryFileDurableAtomic fp ReadWriteMode $ \h -> do
                input <- BS.hGetLine h
                input `shouldBe` hello
                BS.hPut h goodbye
              BS.readFile fp `shouldReturn` (hello <> goodbye)
        bracket (createDirectoryIfMissing True dir >> pure (dir </> "test.file"))
                (const (removeDirectoryRecursive dir))
                testWritingInRelativeDir

  describe "writeBinaryFileDurableAtomic" $
    it "write" $
      withSystemTempDirectory "rio" $ \dir -> do
        let fp = dir </> "with_file_durable_atomic"
        SUT.writeBinaryFileDurableAtomic fp "Hello World"
        BS.readFile fp `shouldReturn` "Hello World"

  describe "withBinaryFileDurable" $
    context "happy path" $
      it "works the same as withFile" $
        withSystemTempDirectory "rio" $ \dir -> do
          let fp = dir </> "with_file_durable"
          SUT.withBinaryFileDurable fp WriteMode $ \h ->
            BS.hPut h "Hello World"
          contents <- BS.readFile fp
          contents `shouldBe` "Hello World"
