package main;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * General Utilities
 */
public class Utils {

    public static final String DEFAULT_DOCUMENT_LABEL = "X";
    public static final String TRAINING_FOLDER = "training";
    public static final String PRE_ANALYSIS_FOLDER = "pre_analysis";
    public static final String DATA_TRAINING_FILENAME = "data_training.txt";
    public static final String INFO_ALL_WORDS_FILENAME = "allwords.txt";
    public static final String INFO_ALL_WORDS_ID_FILENAME = "allwords_ids.csv";
    public static final String INFO_ALL_WORDS_COUNT_FILENAME = "allwordswithcount.csv";
    public static final String DATA_MODEL_MALLET = "model.dat";
    public static final String DATA_MODEL_INSTANCES_MALLET = "instances.dat";
    public static final String TOPIC_KEYS_MALLET = "topic_keys_mallet.txt";
    public static final String TOPIC_COMPOSITION_MALLET = "topic_composition_mallet.txt";
    public static final String EMAIL_REGEX = "([a-zA-Z0-9=*!$&_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+)";
    public static final String URL_REGEX = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";

    /**
     * Cleans a string of punctuation, stop words, other words and urls
     *
     * @param value
     * @param stopWordsPattern
     * @param otherWordsPattern
     * @param wordCountMap
     * @return
     */
    public static String cleanString(String value, String stopWordsPattern, String otherWordsPattern,
                                     HashMap<String, Integer> wordCountMap) {
        Pattern pattern;
        Matcher matcher;

        // Remove stop words
        pattern = Pattern.compile("\\b(?:" + stopWordsPattern + ")\\b\\s*", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(value);
        value = matcher.replaceAll(" ");

        // Remove other words
        pattern = Pattern.compile("\\b(?:" + otherWordsPattern + ")\\b\\s*", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(value);
        value = matcher.replaceAll(" ");

        // Remove URLs
        pattern = Pattern.compile(URL_REGEX, Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(value);
        value = matcher.replaceAll(" ");

        // Remove emails
        pattern = Pattern.compile(EMAIL_REGEX, Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(value);
        value = matcher.replaceAll(" ");

        // Remove numbers
        value = value.replaceAll("\\d+", " ");

        // Remove punctuation and pounds sign
        value = value.replaceAll("\\p{Punct}", " ");
        value = value.trim().replaceAll("£", " ");

        // Remove words with only one character
        value = value.replaceAll("\\s\\w{1,1}\\s", " ");
        value = value.replaceAll("\\s\\w{1,1}\\s", " ");
        value = value.replaceAll("^\\w{1,1}\\s", " ");
        value = value.replaceAll("\\s\\w{1,1}$", " ");

        // Replace extra spaces
        value = value.trim().replaceAll("\\s+", " ");

        // Add processed words to map
        String[] words = value.split(" ");
        for (String word : words) {
            if (wordCountMap.containsKey(word)) {
                int wordCounter = wordCountMap.get(word);
                wordCounter++;
                wordCountMap.put(word, wordCounter);
            } else
                wordCountMap.put(word, 1);
        }

        return value;
    }

    /**
     * Validates if the file or directory exists in the given path
     *
     * @param path Path of the file
     * @return
     */
    public static boolean validateFile(String path) {
        File file = new File(path);
        return file.exists();
    }

    /**
     * Creates a directory given a path
     *
     * @param path Path of the directory
     */
    public static void createDirectory(String path) {
        File file = new File(path);
        file.mkdir();
    }

    /**
     * Returns a BufferedWriter given a path and a file name
     *
     * @param folderPath Folder Path
     * @param fileName   File Name
     * @param append     Indicates if it should append
     * @return
     * @throws IOException
     */
    public static BufferedWriter getBufferedWriter(String folderPath, String fileName, boolean append) throws IOException {
        Utils.createDirectory(folderPath);
        return new BufferedWriter(new FileWriter(new File(Paths.get(folderPath, fileName).toString()), append));
    }

    /**
     * Returns a BufferedReader given a path
     *
     * @param path Path of the file
     * @return
     * @throws FileNotFoundException
     */
    public static BufferedReader getBufferedReader(String path) throws FileNotFoundException {
        return new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
    }

    /**
     * Writes a line followed by a new line to a BufferedWriter
     *
     * @param bw   BufferedWriter to use
     * @param line Line to write
     * @throws IOException
     */
    public static void writeLine(BufferedWriter bw, String line) throws IOException {
        bw.write(line);
        bw.newLine();
    }

    /**
     * Closes a BufferedReader
     *
     * @param br BufferedReader to use
     * @throws IOException
     */
    public static void closeBufferedReader(BufferedReader br) throws IOException {
        if (br != null)
            br.close();
    }

    /**
     * Closes a BufferedWriter
     *
     * @param bw BufferedWriter to use
     * @throws IOException
     */
    public static void closeBufferedWriter(BufferedWriter bw) throws IOException {
        if (bw != null)
            bw.close();
    }

    /**
     * Sorts a map by value, in ascending order
     *
     * @param map
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueAsc(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Sorts a map by value, in descending order
     *
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDesc(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Generates a backup of files within a folder
     *
     * @param folderPath Folder path of files to backup
     */
    public static void generateBackup(String folderPath) {
        if (validateFile(folderPath)) {
            File folder = new File(folderPath);
            File[] files = folder.listFiles();
            for (File file : files) {
                DateFormat dateFormat1 = new SimpleDateFormat("yyyy/MM/dd");
                DateFormat dateFormat2 = new SimpleDateFormat("HHmmss");
                Calendar cal = Calendar.getInstance();
                Path path = Paths.get(folderPath, dateFormat1.format(cal.getTime()));
                //createDirectory(path.toString());
                String backupFolder = path.toString();
                String backupFileName = String.format("%s_%s", dateFormat2.format(cal.getTime()), file.getName());
                System.out.println("backup folder: " + backupFolder);
                System.out.println("backup file: " + backupFileName);
            }
        } else
            System.err.println("Folder not found for backup: " + folderPath);
    }

    /**
     * Checks if a value is duplicated in a given set
     *
     * @param set   Set to find the duplicate in
     * @param value Value to check
     * @return
     */
    public static boolean isValueDuplicated(HashSet set, String value) {
        if (set.contains(value))
            return true;
        else
            set.add(value);
        return false;
    }
}
