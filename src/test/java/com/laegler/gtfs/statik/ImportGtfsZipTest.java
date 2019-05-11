package com.laegler.gtfs.statik;

import static org.junit.Assert.assertThat;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.DataflowPortabilityApiUnsupported;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.CombineFns.CoCombineResult;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * Tests for the {@link ImportGtfsZip} class. These tests make sure that the handling of zip-files
 * works fine.
 */
@RunWith(JUnit4.class)
public class ImportGtfsZipTest {

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private File tmpDir;

  @Rule
  public TemporaryFolder tmpOutputFolder = new TemporaryFolder();
  private File zipFile;

  @Before
  public void setUp() throws Exception {
    tmpDir = tmpFolder.getRoot();
    // zipFile = createZipFileHandle(); // the file is not actually created
  }

  @SuppressWarnings("unchecked")
  @Test
  @Category({ValidatesRunner.class, UsesSideInputs.class, DataflowPortabilityApiUnsupported.class})
  public void testComposedCombineWithContext() {
    p.getCoderRegistry().registerCoderForClass(UserString.class, UserStringCoder.of());

    PCollectionView<String> view = p.apply(Create.of("I")).apply(View.asSingleton());

    PCollection<KV<String, KV<Integer, UserString>>> perKeyInput = p.apply(Create
        .timestamped(Arrays.asList(KV.of("a", KV.of(1, UserString.of("1"))),
            KV.of("a", KV.of(1, UserString.of("1"))), KV.of("a", KV.of(4, UserString.of("4"))),
            KV.of("b", KV.of(1, UserString.of("1"))), KV.of("b", KV.of(13, UserString.of("13")))),
            Arrays.asList(0L, 4L, 7L, 10L, 16L))
        .withCoder(KvCoder.of(StringUtf8Coder.of(),
            KvCoder.of(BigEndianIntegerCoder.of(), UserStringCoder.of()))));

    TupleTag<Integer> maxIntTag = new TupleTag<>();
    TupleTag<UserString> concatStringTag = new TupleTag<>();
    PCollection<KV<String, KV<Integer, String>>> combineGlobally = perKeyInput
        .apply(Values.create())
        .apply(Combine.globally(
            CombineFns.compose().with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag).with(
                new GetUserStringFunction(), new ConcatStringWithContext(view), concatStringTag))
            .withoutDefaults().withSideInputs(ImmutableList.of(view)))
        .apply(WithKeys.of("global")).apply("ExtractGloballyResult",
            ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));

    PCollection<KV<String, KV<Integer, String>>> combinePerKey = perKeyInput
        .apply(Combine.<String, KV<Integer, UserString>, CoCombineResult>perKey(
            CombineFns.compose().with(new GetIntegerFunction(), Max.ofIntegers(), maxIntTag).with(
                new GetUserStringFunction(), new ConcatStringWithContext(view), concatStringTag))
            .withSideInputs(ImmutableList.of(view)))
        .apply("ExtractPerKeyResult", ParDo.of(new ExtractResultDoFn(maxIntTag, concatStringTag)));
    PAssert.that(combineGlobally).containsInAnyOrder(KV.of("global", KV.of(13, "111134I")));
    PAssert.that(combinePerKey).containsInAnyOrder(KV.of("a", KV.of(4, "114I")),
        KV.of("b", KV.of(13, "113I")));
    p.run();
  }

  @SuppressWarnings("unused")
  private static class DisplayDataCombineFn extends Combine.CombineFn<String, String, String> {

    private static final long serialVersionUID = 5000031827290766725L;
    private final String value;
    private static int i;
    private final int id;

    DisplayDataCombineFn(String value) {
      id = ++i;
      this.value = value;
    }

    @Override
    public String createAccumulator() {
      return null;
    }

    @Override
    public String addInput(String accumulator, String input) {
      return null;
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumulators) {
      return null;
    }

    @Override
    public String extractOutput(String accumulator) {
      return null;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("uniqueKey" + id, value))
          .add(DisplayData.item("sharedKey", value));
    }
  }

  private static class UserString implements Serializable {

    private static final long serialVersionUID = 2705390713917682789L;
    private String strValue;

    static UserString of(String strValue) {
      UserString ret = new UserString();
      ret.strValue = strValue;
      return ret;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UserString that = (UserString) o;
      return Objects.equal(strValue, that.strValue);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(strValue);
    }
  }

  private static class UserStringCoder extends AtomicCoder<UserString> {

    private static final long serialVersionUID = 213760148310394757L;

    public static UserStringCoder of() {
      return INSTANCE;
    }

    private static final UserStringCoder INSTANCE = new UserStringCoder();

    @Override
    public void encode(UserString value, OutputStream outStream)
        throws CoderException, IOException {
      encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(UserString value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.strValue, outStream, context);
    }

    @Override
    public UserString decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public UserString decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return UserString.of(StringUtf8Coder.of().decode(inStream, context));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  private static class GetIntegerFunction extends SimpleFunction<KV<Integer, UserString>, Integer> {

    private static final long serialVersionUID = 1001199916041786292L;

    @Override
    public Integer apply(KV<Integer, UserString> input) {
      return input.getKey();
    }
  }

  private static class GetUserStringFunction
      extends SimpleFunction<KV<Integer, UserString>, UserString> {

    private static final long serialVersionUID = 1467402270694566469L;

    @Override
    public UserString apply(KV<Integer, UserString> input) {
      return input.getValue();
    }
  }

  private static class ConcatString extends BinaryCombineFn<UserString> {

    private static final long serialVersionUID = -6495490475187877791L;

    @Override
    public UserString apply(UserString left, UserString right) {
      String retStr = left.strValue + right.strValue;
      char[] chars = retStr.toCharArray();
      Arrays.sort(chars);
      return UserString.of(new String(chars));
    }
  }

  private static class OutputNullString extends BinaryCombineFn<UserString> {

    private static final long serialVersionUID = 7786369351742684218L;

    @Override
    public UserString apply(UserString left, UserString right) {
      return null;
    }
  }

  private static class ConcatStringWithContext
      extends CombineFnWithContext<UserString, UserString, UserString> {

    private static final long serialVersionUID = 424099711300986799L;
    private final PCollectionView<String> view;

    private ConcatStringWithContext(PCollectionView<String> view) {
      this.view = view;
    }

    @Override
    public UserString createAccumulator(CombineWithContext.Context c) {
      return UserString.of(c.sideInput(view));
    }

    @Override
    public UserString addInput(UserString accumulator, UserString input,
        CombineWithContext.Context c) {
      assertThat(accumulator.strValue, Matchers.startsWith(c.sideInput(view)));
      accumulator.strValue += input.strValue;
      return accumulator;
    }

    @Override
    public UserString mergeAccumulators(Iterable<UserString> accumulators,
        CombineWithContext.Context c) {
      String keyPrefix = c.sideInput(view);
      String all = keyPrefix;
      for (UserString accumulator : accumulators) {
        assertThat(accumulator.strValue, Matchers.startsWith(keyPrefix));
        all += accumulator.strValue.substring(keyPrefix.length());
        accumulator.strValue = "cleared in mergeAccumulators";
      }
      return UserString.of(all);
    }

    @Override
    public UserString extractOutput(UserString accumulator, CombineWithContext.Context c) {
      assertThat(accumulator.strValue, Matchers.startsWith(c.sideInput(view)));
      char[] chars = accumulator.strValue.toCharArray();
      Arrays.sort(chars);
      return UserString.of(new String(chars));
    }
  }

  private static class ExtractResultDoFn
      extends DoFn<KV<String, CoCombineResult>, KV<String, KV<Integer, String>>> {


    private static final long serialVersionUID = -2298276190143797288L;
    private final TupleTag<Integer> maxIntTag;
    private final TupleTag<UserString> concatStringTag;

    ExtractResultDoFn(TupleTag<Integer> maxIntTag, TupleTag<UserString> concatStringTag) {
      this.maxIntTag = maxIntTag;
      this.concatStringTag = concatStringTag;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      UserString userString = c.element().getValue().get(concatStringTag);
      KV<Integer, String> value = KV.of(c.element().getValue().get(maxIntTag),
          userString == null ? null : userString.strValue);
      c.output(KV.of(c.element().getKey(), value));
    }
  }

  // /**
  // * Verify that zipping and unzipping works fine. We zip a directory having some subdirectories,
  // * unzip it again and verify the structure to be in place.
  // */
  // @Test
  // public void testZipWithSubdirectories() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // File subDir1 = new File(zipDir, "subDir1");
  // File subDir2 = new File(subDir1, "subdir2");
  // assertTrue(subDir2.mkdirs());
  // createFileWithContents(subDir2, "myTextFile.txt", "Simple Text");
  //
  // assertZipAndUnzipOfDirectoryMatchesOriginal(tmpDir);
  // }
  //
  // /** An empty subdirectory must have its own zip-entry. */
  // @Test
  // public void testEmptySubdirectoryHasZipEntry() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // File subDirEmpty = new File(zipDir, "subDirEmpty");
  // assertTrue(subDirEmpty.mkdirs());
  //
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  // assertZipOnlyContains("zip/subDirEmpty/");
  // }
  //
  // /** A directory with contents should not have a zip entry. */
  // @Test
  // public void testSubdirectoryWithContentsHasNoZipEntry() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // File subDirContent = new File(zipDir, "subdirContent");
  // assertTrue(subDirContent.mkdirs());
  // createFileWithContents(subDirContent, "myTextFile.txt", "Simple Text");
  //
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  // assertZipOnlyContains("zip/subdirContent/myTextFile.txt");
  // }
  //
  // @Test
  // public void testZipDirectoryToOutputStream() throws Exception {
  // createFileWithContents(tmpDir, "myTextFile.txt", "Simple Text");
  // File[] sourceFiles = tmpDir.listFiles();
  // Arrays.sort(sourceFiles);
  // assertThat(sourceFiles, not(arrayWithSize(0)));
  //
  // try (FileOutputStream outputStream = new FileOutputStream(zipFile)) {
  // ZipFiles.zipDirectory(tmpDir, outputStream);
  // }
  // File outputDir = Files.createTempDir();
  // ZipFiles.unzipFile(zipFile, outputDir);
  // File[] outputFiles = outputDir.listFiles();
  // Arrays.sort(outputFiles);
  //
  // assertThat(outputFiles, arrayWithSize(sourceFiles.length));
  // for (int i = 0; i < sourceFiles.length; i++) {
  // compareFileContents(sourceFiles[i], outputFiles[i]);
  // }
  //
  // removeRecursive(outputDir.toPath());
  // assertTrue(zipFile.delete());
  // }
  //
  // @Test
  // public void testEntries() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // File subDir1 = new File(zipDir, "subDir1");
  // File subDir2 = new File(subDir1, "subdir2");
  // assertTrue(subDir2.mkdirs());
  // createFileWithContents(subDir2, "myTextFile.txt", "Simple Text");
  //
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  //
  // try (ZipFile zip = new ZipFile(zipFile)) {
  // Enumeration<? extends ZipEntry> entries = zip.entries();
  // for (ZipEntry entry : ZipFiles.entries(zip)) {
  // assertTrue(entries.hasMoreElements());
  // // ZipEntry doesn't override equals
  // assertEquals(entry.getName(), entries.nextElement().getName());
  // }
  // assertFalse(entries.hasMoreElements());
  // }
  // }
  //
  // @Test
  // public void testAsByteSource() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // assertTrue(zipDir.mkdirs());
  // createFileWithContents(zipDir, "myTextFile.txt", "Simple Text");
  //
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  //
  // try (ZipFile zip = new ZipFile(zipFile)) {
  // ZipEntry entry = zip.getEntry("zip/myTextFile.txt");
  // ByteSource byteSource = ZipFiles.asByteSource(zip, entry);
  // if (entry.getSize() != -1) {
  // assertEquals(entry.getSize(), byteSource.size());
  // }
  // assertArrayEquals("Simple Text".getBytes(StandardCharsets.UTF_8), byteSource.read());
  // }
  // }
  //
  // @Test
  // public void testAsCharSource() throws Exception {
  // File zipDir = new File(tmpDir, "zip");
  // assertTrue(zipDir.mkdirs());
  // createFileWithContents(zipDir, "myTextFile.txt", "Simple Text");
  //
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  //
  // try (ZipFile zip = new ZipFile(zipFile)) {
  // ZipEntry entry = zip.getEntry("zip/myTextFile.txt");
  // CharSource charSource = ZipFiles.asCharSource(zip, entry, StandardCharsets.UTF_8);
  // assertEquals("Simple Text", charSource.read());
  // }
  // }
  //
  // private void assertZipOnlyContains(String zipFileEntry) throws IOException {
  // try (ZipFile zippedFile = new ZipFile(zipFile)) {
  // assertEquals(1, zippedFile.size());
  // ZipEntry entry = zippedFile.entries().nextElement();
  // assertEquals(zipFileEntry, entry.getName());
  // }
  // }
  //
  // /** try to unzip to a non-existent directory and make sure that it fails. */
  // @Test
  // public void testInvalidTargetDirectory() throws IOException {
  // File zipDir = new File(tmpDir, "zipdir");
  // assertTrue(zipDir.mkdir());
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  // File invalidDirectory = new File("/foo/bar");
  // assertTrue(!invalidDirectory.exists());
  // try {
  // ZipFiles.unzipFile(zipFile, invalidDirectory);
  // fail("We expect the IllegalArgumentException, but it never occured");
  // } catch (IllegalArgumentException e) {
  // // This is the expected exception - we passed the test.
  // }
  // }
  //
  // /** Try to unzip to an existing directory, but failing to create directories. */
  // @Test
  // public void testDirectoryCreateFailed() throws IOException {
  // File zipDir = new File(tmpDir, "zipdir");
  // assertTrue(zipDir.mkdir());
  // ZipFiles.zipDirectory(tmpDir, zipFile);
  // File targetDirectory = Files.createTempDir();
  // // Touch a file where the directory should be.
  // Files.touch(new File(targetDirectory, "zipdir"));
  // try {
  // ZipFiles.unzipFile(zipFile, targetDirectory);
  // fail("We expect the IOException, but it never occured");
  // } catch (IOException e) {
  // // This is the expected exception - we passed the test.
  // }
  // }
  //
  // /**
  // * zip and unzip a certain directory, and verify the content afterward to be identical.
  // *
  // * @param sourceDir the directory to zip
  // */
  // private void assertZipAndUnzipOfDirectoryMatchesOriginal(File sourceDir) throws IOException {
  // File[] sourceFiles = sourceDir.listFiles();
  // Arrays.sort(sourceFiles);
  //
  // File zipFile = createZipFileHandle();
  // ZipFiles.zipDirectory(sourceDir, zipFile);
  // File outputDir = Files.createTempDir();
  // ZipFiles.unzipFile(zipFile, outputDir);
  // File[] outputFiles = outputDir.listFiles();
  // Arrays.sort(outputFiles);
  //
  // assertThat(outputFiles, arrayWithSize(sourceFiles.length));
  // for (int i = 0; i < sourceFiles.length; i++) {
  // compareFileContents(sourceFiles[i], outputFiles[i]);
  // }
  //
  // removeRecursive(outputDir.toPath());
  // assertTrue(zipFile.delete());
  // }
  //
  // /**
  // * Compare the content of two files or directories recursively.
  // *
  // * @param expected the expected directory or file content
  // * @param actual the actual directory or file content
  // */
  // private void compareFileContents(File expected, File actual) throws IOException {
  // assertEquals(expected.isDirectory(), actual.isDirectory());
  // assertEquals(expected.getName(), actual.getName());
  // if (expected.isDirectory()) {
  // // Go through the children step by step.
  // File[] expectedChildren = expected.listFiles();
  // Arrays.sort(expectedChildren);
  // File[] actualChildren = actual.listFiles();
  // Arrays.sort(actualChildren);
  // assertThat(actualChildren, arrayWithSize(expectedChildren.length));
  // for (int i = 0; i < expectedChildren.length; i++) {
  // compareFileContents(expectedChildren[i], actualChildren[i]);
  // }
  // } else {
  // // Compare the file content itself.
  // assertTrue(Files.equal(expected, actual));
  // }
  // }
  //
  // /** Create a File object to which we can safely zip a file. */
  // private File createZipFileHandle() throws IOException {
  // File zipFile = File.createTempFile("test", "zip", tmpOutputFolder.getRoot());
  // assertTrue(zipFile.delete());
  // return zipFile;
  // }
  //
  // // This is not generally safe as it does not handle symlinks, etc. However it is safe
  // // enough for these tests.
  // private static void removeRecursive(Path path) throws IOException {
  // Iterable<File> files = Files.fileTreeTraverser().postOrderTraversal(path.toFile());
  // for (File f : files) {
  // java.nio.file.Files.delete(f.toPath());
  // }
  // }
  //
  // /** Create file dir/fileName with contents fileContents. */
  // private void createFileWithContents(File dir, String fileName, String fileContents)
  // throws IOException {
  // File txtFile = new File(dir, fileName);
  // Files.asCharSink(txtFile, StandardCharsets.UTF_8).write(fileContents);
  // }
}
