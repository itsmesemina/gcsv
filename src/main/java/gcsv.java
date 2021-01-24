import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class gcsv {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("gcsv.jar <peptide csv path>");
            return;
        }

        //Stage 1 convert CSV to Fasta format and create Fasta files
        final Path csvPath = Path.of(args[0]);
        final Path parentFolder = csvPath.getParent();
        List<Path> fastaFiles = csvToFasta(csvPath, parentFolder);

        //stage 2 build database if not exist
        System.out.println("Building DB.");
        final String dbPath = parentFolder.resolve("HCMV").toString();
        final String hcmvFastaPath = parentFolder.resolve("HCMVProteins.fasta").toString();
        if (!Files.exists(Path.of(dbPath))) {
            buildDatabase(dbPath, hcmvFastaPath);
        }

        //Stage 3 build result
        System.out.println("Building results");
        List<Path> resultFiles = buildResults(dbPath, fastaFiles, parentFolder);

        //Stage 4 convert result files to fa files
        //mview -in blast result.txt -hsp discrete -out fasta > mv1.fa
        List<Path> faFiles = createFaFiles(resultFiles, parentFolder);
        String faFilesString = faFiles.stream().map(Path::toString).collect(Collectors.joining(" "));

        //Stage 5 open gbench, checks windows and mac dir
        System.out.println("Opening Genome Workbench...");
        final Path windowsGbench = Path.of("C:\\Program Files\\Genome Workbench x64\\bin\\gbench.exe");
        final Path macGbench = Path.of("/Applications/Genome\\ Workbench.app/Contents/MacOS/Genome\\ Workbench");
        if(Files.exists(windowsGbench)){
            Runtime.getRuntime().exec("\"" + windowsGbench.toString() + "\"" + " " + faFilesString);
        } else if(Files.exists(macGbench)){
            Runtime.getRuntime().exec(macGbench.toString() + " " + faFilesString);
        } else {
            System.out.println("Genome Workbench not found");
        }
    }

    private static List<Path> csvToFasta(Path inputFile, Path parentFolder) {
        final String DELIMITER = ",";
        final String PREFIX = ">pep";
        AtomicInteger peptideCount = new AtomicInteger(1);
        final int PEPTIDE_COL_INDEX = 25;

        try (BufferedReader csvReader = Files.newBufferedReader(inputFile, StandardCharsets.ISO_8859_1)) {
            return csvReader.lines().skip(1)                                            //skip header
                    .map(line -> line.split(DELIMITER))
                    .map(lineArray -> lineArray[PEPTIDE_COL_INDEX])
                    .map(peptide -> PREFIX + peptideCount.getAndIncrement() + System.lineSeparator() + peptide) //convert CSV format to Fasta
                    .map(fastaData -> createFastaFile(fastaData, parentFolder.resolve("pep" + peptideCount.get() + ".fasta")))
                    .collect(Collectors.toUnmodifiableList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public static Path createFastaFile(String fastaData, Path outputFile){
        try {
            return Files.writeString(outputFile, fastaData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outputFile;
    }

    //makeblastdb -in HCMVProteins.fasta -dbtype prot -out C:/out/HCMV -title "HCMV genome" -parse_seqids -blastdb_version 4
    private static void buildDatabase(String dbPath, String hcmvFastaPath) throws IOException {
        final String[] buildDbCmd = {"makeblastdb", "-in", hcmvFastaPath, "-dbtype", "prot", "-out", dbPath,
                "-title", "\"HCMV genome\"", "-parse_seqids", "-blastdb_version", "4"};
        Runtime.getRuntime().exec(buildDbCmd);
    }

    //blastp -db HCMV -query peptides.fasta -out result.txt
    private static List<Path> buildResults(String dbPath, List<Path> fastaFiles, Path parentFolder) throws IOException {
        List<Path> resultFiles = new ArrayList<>();
        int resultCount = 1;
        for (Path fastaFile : fastaFiles ) {
            Path resultFile = parentFolder.resolve("result" + resultCount++ + ".txt");
            String[] buildResult = {"blastp", "-db", dbPath, "-query", fastaFile.toString(), "-outfmt", "0", "-out", resultFile.toString()};
            Runtime.getRuntime().exec(buildResult);
            resultFiles.add(resultFile);
        }
        return resultFiles;
    }

  // mview -in blast result.txt -hsp discrete -out fasta > mv1.fa
  private static List<Path> createFaFiles(List<Path> resultFiles, Path parentFolder) throws IOException {
        List<Path> faFiles = new ArrayList<>();
        int faCount = 1;
        for (Path resultFile : resultFiles ) {
            Path faFile = parentFolder.resolve("mv" + faCount++ + ".fa");
            List<String> mviewCmd = new ArrayList<>(List.of("mview", "-in", "blast", resultFile.toString(), "-hsp", "discrete", "-minident", "100", "-out", "fasta", ">", faFile.toString()));

            if(System.getProperty("os.name").contains("Windows")){
                mviewCmd.addAll(0, List.of("cmd", "/c") );
            }

            Runtime.getRuntime().exec(mviewCmd.toArray(String[]::new));
            faFiles.add(faFile);
        }

        return faFiles;
    }
}
