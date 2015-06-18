/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import scala.util.Random

import java.io.{File, ByteArrayOutputStream, ByteArrayInputStream, FileOutputStream}
import java.net.{BindException, ServerSocket, URI}
import java.nio.{ByteBuffer, ByteOrder}
import java.text.DecimalFormatSymbols
import java.util.Locale

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}

class UtilsSuite extends SparkFunSuite with ResetSystemProperties {

  test("bytesToString") {
    assert(Utils.bytesToString(10) === "10.0 B")
    assert(Utils.bytesToString(1500) === "1500.0 B")
    assert(Utils.bytesToString(2000000) === "1953.1 KB")
    assert(Utils.bytesToString(2097152) === "2.0 MB")
    assert(Utils.bytesToString(2306867) === "2.2 MB")
    assert(Utils.bytesToString(5368709120L) === "5.0 GB")
    assert(Utils.bytesToString(5L * 1024L * 1024L * 1024L * 1024L) === "5.0 TB")
  }

  test("copyStream") {
    // input array initialization
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }

  test("memoryStringToMb") {
    assert(Utils.memoryStringToMb("1") === 0)
    assert(Utils.memoryStringToMb("1048575") === 0)
    assert(Utils.memoryStringToMb("3145728") === 3)

    assert(Utils.memoryStringToMb("1024k") === 1)
    assert(Utils.memoryStringToMb("5000k") === 4)
    assert(Utils.memoryStringToMb("4024k") === Utils.memoryStringToMb("4024K"))

    assert(Utils.memoryStringToMb("1024m") === 1024)
    assert(Utils.memoryStringToMb("5000m") === 5000)
    assert(Utils.memoryStringToMb("4024m") === Utils.memoryStringToMb("4024M"))

    assert(Utils.memoryStringToMb("2g") === 2048)
    assert(Utils.memoryStringToMb("3g") === Utils.memoryStringToMb("3G"))

    assert(Utils.memoryStringToMb("2t") === 2097152)
    assert(Utils.memoryStringToMb("3t") === Utils.memoryStringToMb("3T"))
  }

  test("splitCommandString") {
    assert(Utils.splitCommandString("") === Seq())
    assert(Utils.splitCommandString("a") === Seq("a"))
    assert(Utils.splitCommandString("aaa") === Seq("aaa"))
    assert(Utils.splitCommandString("a b c") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("  a   b\t c ") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("a 'b c'") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a 'b c' d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("'b c'") === Seq("b c"))
    assert(Utils.splitCommandString("a \"b c\"") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a \"b c\" d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("\"b c\"") === Seq("b c"))
    assert(Utils.splitCommandString("a 'b\" c' \"d' e\"") === Seq("a", "b\" c", "d' e"))
    assert(Utils.splitCommandString("a\t'b\nc'\nd") === Seq("a", "b\nc", "d"))
    assert(Utils.splitCommandString("a \"b\\\\c\"") === Seq("a", "b\\c"))
    assert(Utils.splitCommandString("a \"b\\\"c\"") === Seq("a", "b\"c"))
    assert(Utils.splitCommandString("a 'b\\\"c'") === Seq("a", "b\\\"c"))
    assert(Utils.splitCommandString("'a'b") === Seq("ab"))
    assert(Utils.splitCommandString("'a''b'") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"b") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"\"b\"") === Seq("ab"))
    assert(Utils.splitCommandString("''") === Seq(""))
    assert(Utils.splitCommandString("\"\"") === Seq(""))
  }

  test("string formatting of time durations") {
    val second = 1000
    val minute = second * 60
    val hour = minute * 60
    def str = Utils.msDurationToString(_)

    val sep = new DecimalFormatSymbols(Locale.getDefault()).getDecimalSeparator()

    assert(str(123) === "123 ms")
    assert(str(second) === "1" + sep + "0 s")
    assert(str(second + 462) === "1" + sep + "5 s")
    assert(str(hour) === "1" + sep + "00 h")
    assert(str(minute) === "1" + sep + "0 m")
    assert(str(minute + 4 * second + 34) === "1" + sep + "1 m")
    assert(str(10 * hour + minute + 4 * second) === "10" + sep + "02 h")
    assert(str(10 * hour + 59 * minute + 59 * second + 999) === "11" + sep + "00 h")
  }

  test("reading offset bytes of a file") {
    val tmpDir2 = Utils.createTempDir()
    tmpDir2.deleteOnExit()
    val f1Path = tmpDir2 + "/f1"
    val f1 = new FileOutputStream(f1Path)
    f1.write("1\n2\n3\n4\n5\n6\n7\n8\n9\n".getBytes(UTF_8))
    f1.close()

    // Read first few bytes
    assert(Utils.offsetBytes(f1Path, 0, 5) === "1\n2\n3")

    // Read some middle bytes
    assert(Utils.offsetBytes(f1Path, 4, 11) === "3\n4\n5\n6")

    // Read last few bytes
    assert(Utils.offsetBytes(f1Path, 12, 18) === "7\n8\n9\n")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(f1Path, -5, 5) === "1\n2\n3")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(f1Path, 12, 22) === "7\n8\n9\n")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(f1Path, -3, 25) === "1\n2\n3\n4\n5\n6\n7\n8\n9\n")

    Utils.deleteRecursively(tmpDir2)
  }

  test("reading offset bytes across multiple files") {
    val tmpDir = Utils.createTempDir()
    tmpDir.deleteOnExit()
    val files = (1 to 3).map(i => new File(tmpDir, i.toString))
    Files.write("0123456789", files(0), UTF_8)
    Files.write("abcdefghij", files(1), UTF_8)
    Files.write("ABCDEFGHIJ", files(2), UTF_8)

    // Read first few bytes in the 1st file
    assert(Utils.offsetBytes(files, 0, 5) === "01234")

    // Read bytes within the 1st file
    assert(Utils.offsetBytes(files, 5, 8) === "567")

    // Read bytes across 1st and 2nd file
    assert(Utils.offsetBytes(files, 8, 18) === "89abcdefgh")

    // Read bytes across 1st, 2nd and 3rd file
    assert(Utils.offsetBytes(files, 5, 24) === "56789abcdefghijABCD")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(files, -5, 18) === "0123456789abcdefgh")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(files, 18, 35) === "ijABCDEFGHIJ")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(files, -5, 35) === "0123456789abcdefghijABCDEFGHIJ")

    Utils.deleteRecursively(tmpDir)
  }

  test("deserialize long value") {
    val testval : Long = 9730889947L
    val bbuf = ByteBuffer.allocate(8)
    assert(bbuf.hasArray)
    bbuf.order(ByteOrder.BIG_ENDIAN)
    bbuf.putLong(testval)
    assert(bbuf.array.length === 8)
    assert(Utils.deserializeLongValue(bbuf.array) === testval)
  }

  test("get iterator size") {
    val empty = Seq[Int]()
    assert(Utils.getIteratorSize(empty.toIterator) === 0L)
    val iterator = Iterator.range(0, 5)
    assert(Utils.getIteratorSize(iterator) === 5L)
  }

  test("doesDirectoryContainFilesNewerThan") {
    // create some temporary directories and files
    val parent: File = Utils.createTempDir()
    val child1: File = Utils.createTempDir(parent.getCanonicalPath) // The parent directory has two child directories
    val child2: File = Utils.createTempDir(parent.getCanonicalPath)
    val child3: File = Utils.createTempDir(child1.getCanonicalPath)
    // set the last modified time of child1 to 30 secs old
    child1.setLastModified(System.currentTimeMillis() - (1000 * 30))

    // although child1 is old, child2 is still new so return true
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child2.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    parent.setLastModified(System.currentTimeMillis - (1000 * 30))
    // although parent and its immediate children are new, child3 is still old
    // we expect a full recursive search for new files.
    assert(Utils.doesDirectoryContainAnyNewFiles(parent, 5))

    child3.setLastModified(System.currentTimeMillis - (1000 * 30))
    assert(!Utils.doesDirectoryContainAnyNewFiles(parent, 5))
  }

  test("resolveURI") {
    def assertResolves(before: String, after: String, testWindows: Boolean = false): Unit = {
      // This should test only single paths
      assume(before.split(",").length === 1)
      // Repeated invocations of resolveURI should yield the same result
      def resolve(uri: String): String = Utils.resolveURI(uri, testWindows).toString
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
      // Also test resolveURIs with single paths
      assert(new URI(Utils.resolveURIs(before, testWindows)) === new URI(after))
      assert(new URI(Utils.resolveURIs(after, testWindows)) === new URI(after))
    }
    val cwd = System.getProperty("user.dir")
    assertResolves("hdfs:/root/spark.jar", "hdfs:/root/spark.jar")
    assertResolves("hdfs:///root/spark.jar#app.jar", "hdfs:/root/spark.jar#app.jar")
    assertResolves("spark.jar", s"file:$cwd/spark.jar")
    assertResolves("spark.jar#app.jar", s"file:$cwd/spark.jar#app.jar")
    assertResolves("C:/path/to/file.txt", "file:/C:/path/to/file.txt", testWindows = true)
    assertResolves("C:\\path\\to\\file.txt", "file:/C:/path/to/file.txt", testWindows = true)
    assertResolves("file:/C:/path/to/file.txt", "file:/C:/path/to/file.txt", testWindows = true)
    assertResolves("file:///C:/path/to/file.txt", "file:/C:/path/to/file.txt", testWindows = true)
    assertResolves("file:/C:/file.txt#alias.txt", "file:/C:/file.txt#alias.txt", testWindows = true)
    intercept[IllegalArgumentException] { Utils.resolveURI("file:foo") }
    intercept[IllegalArgumentException] { Utils.resolveURI("file:foo:baby") }
  }

  test("resolveURIs with multiple paths") {
    def assertResolves(before: String, after: String, testWindows: Boolean = false): Unit = {
      assume(before.split(",").length > 1)
      assert(Utils.resolveURIs(before, testWindows) === after)
      assert(Utils.resolveURIs(after, testWindows) === after)
      // Repeated invocations of resolveURIs should yield the same result
      def resolve(uri: String): String = Utils.resolveURIs(uri, testWindows)
      assert(resolve(after) === after)
      assert(resolve(resolve(after)) === after)
      assert(resolve(resolve(resolve(after))) === after)
    }
    val cwd = System.getProperty("user.dir")
    assertResolves("jar1,jar2", s"file:$cwd/jar1,file:$cwd/jar2")
    assertResolves("file:/jar1,file:/jar2", "file:/jar1,file:/jar2")
    assertResolves("hdfs:/jar1,file:/jar2,jar3", s"hdfs:/jar1,file:/jar2,file:$cwd/jar3")
    assertResolves("hdfs:/jar1,file:/jar2,jar3,jar4#jar5",
      s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:$cwd/jar4#jar5")
    assertResolves("hdfs:/jar1,file:/jar2,jar3,C:\\pi.py#py.pi",
      s"hdfs:/jar1,file:/jar2,file:$cwd/jar3,file:/C:/pi.py#py.pi", testWindows = true)
  }

  test("nonLocalPaths") {
    assert(Utils.nonLocalPaths("spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("file:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("file:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("local:///spark.jar") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar") === Array("hdfs:/spark.jar"))
    assert(Utils.nonLocalPaths("hdfs:///spark.jar") === Array("hdfs:///spark.jar"))
    assert(Utils.nonLocalPaths("file:/spark.jar,local:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("local:/spark.jar,file:/smart.jar,family.py") === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("hdfs:/spark.jar,s3:/smart.jar,local.py,file:/hello/pi.py") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))
    assert(Utils.nonLocalPaths("local.py,hdfs:/spark.jar,file:/hello/pi.py,s3:/smart.jar") ===
      Array("hdfs:/spark.jar", "s3:/smart.jar"))

    // Test Windows paths
    assert(Utils.nonLocalPaths("C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("file:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:/C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("local:///C:/some/path.jar", testWindows = true) === Array.empty)
    assert(Utils.nonLocalPaths("hdfs:/a.jar,C:/my.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("D:/your.jar,hdfs:/a.jar,s3:/another.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
    assert(Utils.nonLocalPaths("hdfs:/a.jar,s3:/another.jar,e:/our.jar", testWindows = true) ===
      Array("hdfs:/a.jar", "s3:/another.jar"))
  }

  test("isBindCollision") {
    // Negatives
    assert(!Utils.isBindCollision(null))
    assert(!Utils.isBindCollision(new Exception))
    assert(!Utils.isBindCollision(new Exception(new Exception)))
    assert(!Utils.isBindCollision(new Exception(new BindException)))

    // Positives
    val be = new BindException("Random Message")
    val be1 = new Exception(new BindException("Random Message"))
    val be2 = new Exception(new Exception(new BindException("Random Message")))
    assert(Utils.isBindCollision(be))
    assert(Utils.isBindCollision(be1))
    assert(Utils.isBindCollision(be2))

    // Actual bind exception
    var server1: ServerSocket = null
    var server2: ServerSocket = null
    try {
      server1 = new java.net.ServerSocket(0)
      server2 = new java.net.ServerSocket(server1.getLocalPort)
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[java.net.BindException])
        assert(Utils.isBindCollision(e))
    } finally {
      Option(server1).foreach(_.close())
      Option(server2).foreach(_.close())
    }
  }

  test("deleteRecursively") {
    val tempDir1 = Utils.createTempDir()
    assert(tempDir1.exists())
    Utils.deleteRecursively(tempDir1)
    assert(!tempDir1.exists())

    val tempDir2 = Utils.createTempDir()
    val sourceFile1 = new File(tempDir2, "foo.txt")
    Files.touch(sourceFile1)
    assert(sourceFile1.exists())
    Utils.deleteRecursively(sourceFile1)
    assert(!sourceFile1.exists())

    val tempDir3 = new File(tempDir2, "subdir")
    assert(tempDir3.mkdir())
    val sourceFile2 = new File(tempDir3, "bar.txt")
    Files.touch(sourceFile2)
    assert(sourceFile2.exists())
    Utils.deleteRecursively(tempDir2)
    assert(!tempDir2.exists())
    assert(!tempDir3.exists())
    assert(!sourceFile2.exists())
  }

  test("loading properties from file") {
    val outFile = File.createTempFile("test-load-spark-properties", "test")
    try {
      System.setProperty("spark.test.fileNameLoadB", "2")
      Files.write("spark.test.fileNameLoadA true\n" +
        "spark.test.fileNameLoadB 1\n", outFile, UTF_8)
      val properties = Utils.getPropertiesFromFile(outFile.getAbsolutePath)
      properties
        .filter { case (k, v) => k.startsWith("spark.")}
        .foreach { case (k, v) => sys.props.getOrElseUpdate(k, v)}
      val sparkConf = new SparkConf
      assert(sparkConf.getBoolean("spark.test.fileNameLoadA", false) === true)
      assert(sparkConf.getInt("spark.test.fileNameLoadB", 1) === 2)
    } finally {
      outFile.delete()
    }
  }

  test("timeIt with prepare") {
    var cnt = 0
    val prepare = () => {
      cnt += 1
      Thread.sleep(1000)
    }
    val time = Utils.timeIt(2)({}, Some(prepare))
    require(cnt === 2, "prepare should be called twice")
    require(time < 500, "preparation time should not count")
  }

  test("fetch hcfs dir") {
    val sourceDir = Utils.createTempDir()
    val innerSourceDir = Utils.createTempDir(root=sourceDir.getPath)
    val sourceFile = File.createTempFile("someprefix", "somesuffix", innerSourceDir)
    val targetDir = new File(Utils.createTempDir(), "target-dir")
    Files.write("some text", sourceFile, UTF_8)

    val path = new Path("file://" + sourceDir.getAbsolutePath)
    val conf = new Configuration()
    val fs = Utils.getHadoopFileSystem(path.toString, conf)

    assert(!targetDir.isDirectory())
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)
    assert(targetDir.isDirectory())

    // Copy again to make sure it doesn't error if the dir already exists.
    Utils.fetchHcfsFile(path, targetDir, fs, new SparkConf(), conf, false)

    val destDir = new File(targetDir, sourceDir.getName())
    assert(destDir.isDirectory())

    val destInnerDir = new File(destDir, innerSourceDir.getName)
    assert(destInnerDir.isDirectory())

    val destInnerFile = new File(destInnerDir, sourceFile.getName)
    assert(destInnerFile.isFile())

    val filePath = new Path("file://" + sourceFile.getAbsolutePath)
    val testFileDir = new File("test-filename")
    val testFileName = "testFName"
    val testFilefs = Utils.getHadoopFileSystem(filePath.toString, conf)
    Utils.fetchHcfsFile(filePath, testFileDir, testFilefs, new SparkConf(),
                        conf, false, Some(testFileName))
    val newFileName = new File(testFileDir, testFileName)
    assert(newFileName.isFile())
  }

  test("isInDirectory") {
    val tmpDir = new File(sys.props("java.io.tmpdir"))
    val parentDir = new File(tmpDir, "parent-dir")
    val childDir1 = new File(parentDir, "child-dir-1")
    val childDir1b = new File(parentDir, "child-dir-1b")
    val childFile1 = new File(parentDir, "child-file-1.txt")
    val childDir2 = new File(childDir1, "child-dir-2")
    val childDir2b = new File(childDir1, "child-dir-2b")
    val childFile2 = new File(childDir1, "child-file-2.txt")
    val childFile3 = new File(childDir2, "child-file-3.txt")
    val nullFile: File = null
    parentDir.mkdir()
    childDir1.mkdir()
    childDir1b.mkdir()
    childDir2.mkdir()
    childDir2b.mkdir()
    childFile1.createNewFile()
    childFile2.createNewFile()
    childFile3.createNewFile()

    // Identity
    assert(Utils.isInDirectory(parentDir, parentDir))
    assert(Utils.isInDirectory(childDir1, childDir1))
    assert(Utils.isInDirectory(childDir2, childDir2))

    // Valid ancestor-descendant pairs
    assert(Utils.isInDirectory(parentDir, childDir1))
    assert(Utils.isInDirectory(parentDir, childFile1))
    assert(Utils.isInDirectory(parentDir, childDir2))
    assert(Utils.isInDirectory(parentDir, childFile2))
    assert(Utils.isInDirectory(parentDir, childFile3))
    assert(Utils.isInDirectory(childDir1, childDir2))
    assert(Utils.isInDirectory(childDir1, childFile2))
    assert(Utils.isInDirectory(childDir1, childFile3))
    assert(Utils.isInDirectory(childDir2, childFile3))

    // Inverted ancestor-descendant pairs should fail
    assert(!Utils.isInDirectory(childDir1, parentDir))
    assert(!Utils.isInDirectory(childDir2, parentDir))
    assert(!Utils.isInDirectory(childDir2, childDir1))
    assert(!Utils.isInDirectory(childFile1, parentDir))
    assert(!Utils.isInDirectory(childFile2, parentDir))
    assert(!Utils.isInDirectory(childFile3, parentDir))
    assert(!Utils.isInDirectory(childFile2, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir1))
    assert(!Utils.isInDirectory(childFile3, childDir2))

    // Non-existent files or directories should fail
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two.txt")))
    assert(!Utils.isInDirectory(parentDir, new File(parentDir, "one/two/three.txt")))

    // Siblings should fail
    assert(!Utils.isInDirectory(childDir1, childDir1b))
    assert(!Utils.isInDirectory(childDir1, childFile1))
    assert(!Utils.isInDirectory(childDir2, childDir2b))
    assert(!Utils.isInDirectory(childDir2, childFile2))

    // Null files should fail without throwing NPE
    assert(!Utils.isInDirectory(parentDir, nullFile))
    assert(!Utils.isInDirectory(childFile3, nullFile))
    assert(!Utils.isInDirectory(nullFile, parentDir))
    assert(!Utils.isInDirectory(nullFile, childFile3))
  }
}
