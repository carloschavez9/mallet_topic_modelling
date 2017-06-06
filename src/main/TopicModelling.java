package main;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

import cc.mallet.types.*;
import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.*;
import cc.mallet.topics.*;

public class TopicModelling {

    private HashSet<String> idList_;
    private HashMap<String, Integer> wordCountMap_;
    private HashMap<String, String> idAdviceCodeLevel1Map_;
    private HashMap<String, String> idAdviceCodeLevel2Map_;
    private HashMap<String, String> idAdviceCodeLevel3Map_;

    private String stopWordsStringPattern_;
    private String otherWordsStringPattern_;

    /**
     * Creates a new text file splitter. Loads files for stop words and other words
     *
     * @param stopWordsPath  Path of file containing stop words
     * @param otherWordsPath Path of file containing other words
     * @throws Exception
     */
    public TopicModelling(String stopWordsPath, String otherWordsPath) throws Exception {
        idList_ = new HashSet<String>();
        wordCountMap_ = new HashMap<String, Integer>();
        idAdviceCodeLevel1Map_ = new HashMap<String, String>();
        idAdviceCodeLevel2Map_ = new HashMap<String, String>();
        idAdviceCodeLevel3Map_ = new HashMap<String, String>();

        loadFiles(stopWordsPath, otherWordsPath);
    }

    /**
     * Loads advice code ids
     *
     * @param path
     * @param splitChar
     * @param skipFirstLine
     * @throws Exception
     */
    public void loadAdviceCodeIds(String path, String splitChar, boolean skipFirstLine) throws Exception {
        BufferedReader brInput = null;
        try {
            // advice codes
            if (Utils.validateFile(path)) {
                // Load
                brInput = Utils.getBufferedReader(path);
                String inputLine = brInput.readLine();
                // skip first line
                if (skipFirstLine)
                    inputLine = brInput.readLine();
                // Read lines
                while (inputLine != null) {
                    // Do not process empty lines or lines beginning with #
                    if (inputLine.isEmpty() || inputLine.startsWith("#")) {
                        inputLine = brInput.readLine();
                        continue;
                    }

                    String[] values = inputLine.split(splitChar, -1);
                    String enquiryNumber = values[0].trim();
                    String adviceCodeLevel1 = values[1].trim();
                    String adviceCodeLevel2 = values[2].trim();
                    String adviceCodeLevel3 = values[3].trim();

                    // add Advice code level 1
                    if (!adviceCodeLevel1.isEmpty())
                        idAdviceCodeLevel1Map_.put(enquiryNumber, adviceCodeLevel1);
                    // add Advice code level 2
                    if (!adviceCodeLevel2.isEmpty())
                        idAdviceCodeLevel2Map_.put(enquiryNumber, adviceCodeLevel2);
                    // add Advice code level 3
                    if (!adviceCodeLevel3.isEmpty())
                        idAdviceCodeLevel3Map_.put(enquiryNumber, adviceCodeLevel3);

                    inputLine = brInput.readLine();
                }
            } else
                System.err.println("File not found: " + path);

        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                // Close files
                Utils.closeBufferedReader(brInput);
            } catch (Exception ex) {
                // Don't do anything
            }
        }
    }

    /**
     * Loads the stop words and other words files to create the string patterns
     *
     * @param stopWordsPath  Path of file containing stop words
     * @param otherWordsPath Path of file containing other words
     * @throws Exception
     */
    private void loadFiles(String stopWordsPath, String otherWordsPath) throws Exception {
        BufferedReader brStopWordsInput = null;
        BufferedReader brOtherWordsInput = null;
        try {
            ArrayList<String> wordList = new ArrayList<String>();
            String inputLine;

            // Stop words
            if (Utils.validateFile(stopWordsPath)) {
                // Load
                brStopWordsInput = Utils.getBufferedReader(stopWordsPath);
                inputLine = brStopWordsInput.readLine();
                // Read lines
                while (inputLine != null) {
                    // Do not process empty lines or lines beginning with #
                    if (inputLine.isEmpty() || inputLine.startsWith("#")) {
                        inputLine = brStopWordsInput.readLine();
                        continue;
                    }
                    // Add line to list
                    wordList.add(inputLine.trim());
                    inputLine = brStopWordsInput.readLine();
                }
                stopWordsStringPattern_ = String.join("|", wordList);
            } else
                System.err.println("File not found: " + stopWordsPath);

            // Other words
            if (Utils.validateFile(otherWordsPath)) {
                wordList = new ArrayList<String>();
                // Load
                brOtherWordsInput = Utils.getBufferedReader(otherWordsPath);
                inputLine = brOtherWordsInput.readLine();
                // Read lines
                while (inputLine != null) {
                    // Do not process empty lines or lines beginning with #
                    if (inputLine.isEmpty() || inputLine.startsWith("#")) {
                        inputLine = brOtherWordsInput.readLine();
                        continue;
                    }
                    // Add line to list
                    wordList.add(inputLine.trim());
                    inputLine = brOtherWordsInput.readLine();
                }

                otherWordsStringPattern_ = String.join("|", wordList);
            } else
                System.err.println("File not found: " + otherWordsPath);
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                // Close files
                Utils.closeBufferedReader(brStopWordsInput);
                Utils.closeBufferedReader(brOtherWordsInput);
            } catch (Exception ex) {
                // Don't do anything
            }
        }
    }

    /**
     * Generates the training file for Mallet, one file with all the processed words per line, and one file with all the processed words per line plus the id
     *
     * @param folderPath
     * @param fileName
     * @param splitChar
     * @param fieldId
     * @param primaryFieldNumber
     * @param firstExtraField
     * @param lastExtraField
     * @param includeExtraFields
     * @param ignoreDuplicates
     * @param skipFirstLine
     * @param backupFiles
     * @throws Exception
     */
    public void generateTrainingFile(String folderPath, String fileName, String splitChar, int fieldId, int primaryFieldNumber, int firstExtraField, int lastExtraField, boolean includeExtraFields, boolean ignoreDuplicates,
                                     boolean skipFirstLine, boolean backupFiles) throws Exception {
        BufferedReader brInput = null;
        BufferedWriter bwOutputAllWords = null;
        BufferedWriter bwOutputAllWordsWithId = null;
        BufferedWriter bwOutputAllWordsWithCount = null;
        BufferedWriter bwOutputTraining = null;
        try {
            String completePath = Paths.get(folderPath, fileName).toString();
            if (Utils.validateFile(completePath)) {
                if (backupFiles)
                    Utils.generateBackup(Paths.get(folderPath, Utils.TRAINING_FOLDER).toString());

                // File for all words
                bwOutputAllWords = Utils.getBufferedWriter(Paths.get(folderPath, Utils.PRE_ANALYSIS_FOLDER).toString(), Utils.INFO_ALL_WORDS_FILENAME, false);
                // File for all words with id per document. id, words in primary field, words in extra fields
                bwOutputAllWordsWithId = Utils.getBufferedWriter(Paths.get(folderPath, Utils.PRE_ANALYSIS_FOLDER).toString(), Utils.INFO_ALL_WORDS_ID_FILENAME, false);
                Utils.writeLine(bwOutputAllWordsWithId, "id,primary_field_words,extra_fields_words");
                // File for all words with count
                bwOutputAllWordsWithCount = Utils.getBufferedWriter(Paths.get(folderPath, Utils.PRE_ANALYSIS_FOLDER).toString(), Utils.INFO_ALL_WORDS_COUNT_FILENAME, false);
                Utils.writeLine(bwOutputAllWordsWithCount, "word,count");
                // File for all documents per line for training
                bwOutputTraining = Utils.getBufferedWriter(Paths.get(folderPath, Utils.TRAINING_FOLDER).toString(), Utils.DATA_TRAINING_FILENAME, false);

                // File to read
                brInput = Utils.getBufferedReader(completePath);

                String inputLine = brInput.readLine();
                // skip first line
                if (skipFirstLine)
                    inputLine = brInput.readLine();
                // Read lines
                while (inputLine != null) {
                    // Do not process empty lines or lines beginning with #
                    if (inputLine.isEmpty() || inputLine.startsWith("#")) {
                        inputLine = brInput.readLine();
                        continue;
                    }

                    // To lowercase
                    inputLine = inputLine.toLowerCase();

                    String[] values = inputLine.split(splitChar, -1);
                    String documentID = values[fieldId].trim();

                    // Ignore duplicates, continue with next line
                    if (ignoreDuplicates) {
                        if (Utils.isValueDuplicated(idList_, documentID)) {
                            inputLine = brInput.readLine();
                            continue;
                        }
                    }

                    // - Process primary Field value
                    String primaryFieldValue = values[primaryFieldNumber].trim();
                    // Ignore empty value
                    if (primaryFieldValue.isEmpty()) {
                        inputLine = brInput.readLine();
                        continue;
                    }

                    // Clean string
                    primaryFieldValue = Utils.cleanString(primaryFieldValue, stopWordsStringPattern_, otherWordsStringPattern_, wordCountMap_);

                    // - Process other Field value
                    // Don't process Fields with duplicate values
                    HashSet<String> otherFieldValues = new HashSet<String>();
                    for (int i = firstExtraField; i < lastExtraField; i++) {
                        otherFieldValues.add(values[i].trim());
                    }

                    // Concat all other values
                    String otherFieldValue = "";
                    for (String val : otherFieldValues) {
                        otherFieldValue += val + " ";
                    }

                    // Clean string
                    otherFieldValue = Utils.cleanString(otherFieldValue, stopWordsStringPattern_, otherWordsStringPattern_, wordCountMap_);

                    // Add primary field value without extra fields
                    if (!includeExtraFields)
                        Utils.writeLine(bwOutputTraining, String.format("%s,%s,%s", documentID, Utils.DEFAULT_DOCUMENT_LABEL, primaryFieldValue));
                        // Add primary field value with extra fields
                    else
                        Utils.writeLine(bwOutputTraining, String.format("%s,%s,%s %s", documentID, Utils.DEFAULT_DOCUMENT_LABEL, primaryFieldValue, otherFieldValue));

                    // Add info of primaryFieldValue and otherFieldValue to file
                    Utils.writeLine(bwOutputAllWords, String.format("%s %s", primaryFieldValue, otherFieldValue));
                    // Add info of id, primaryFieldValue and otherFieldValue to file
                    Utils.writeLine(bwOutputAllWordsWithId, String.format("%s,%s,%s", documentID, primaryFieldValue, otherFieldValue));

                    // Read next line
                    inputLine = brInput.readLine();
                }

                // Add words with count to file
                wordCountMap_ = (HashMap<String, Integer>) Utils.sortByValueDesc(wordCountMap_);
                for (String word : wordCountMap_.keySet()) {
                    Utils.writeLine(bwOutputAllWordsWithCount, String.format("%s,%d", word, wordCountMap_.get(word)));
                }
            } else
                System.err.println("File not found: " + completePath);
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                // Close files
                Utils.closeBufferedReader(brInput);
                Utils.closeBufferedWriter(bwOutputAllWords);
                Utils.closeBufferedWriter(bwOutputAllWordsWithId);
                Utils.closeBufferedWriter(bwOutputAllWordsWithCount);
                Utils.closeBufferedWriter(bwOutputTraining);
            } catch (Exception ex) {
                // Don't do anything
            }
        }
    }

    /**
     * Trains a model for topic modelling using mallet
     *
     * @param folderPath
     * @param trainingFileName
     * @param numTopics
     * @throws IOException
     */
    public void trainTopicModellingUsingMallet(String folderPath, String trainingFileName, int numTopics) throws IOException {
        // Begin by importing documents from text to feature sequences
        ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

        // Pipes: tokenize, map to features
        pipeList.add(new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")));
        pipeList.add(new TokenSequence2FeatureSequence());

        InstanceList instances = new InstanceList(new SerialPipes(pipeList));
        Reader fileReader = Utils.getBufferedReader(Paths.get(folderPath, Utils.TRAINING_FOLDER, trainingFileName).toString());
        instances.addThruPipe(new CsvIterator(fileReader, Pattern.compile("^(\\S*)[\\s,]*(\\S*)[\\s,]*(.*)$"),
                3, 2, 1)); // data, label, name fields

        // Create a model with topics, add instances
        ParallelTopicModel model = new ParallelTopicModel(numTopics);
        model.addInstances(instances);
        // Use two parallel samplers, which each look at one half the corpus and combine
        //  statistics after every iteration.
        model.setNumThreads(2);
        model.setRandomSeed(1); // To replicate results

        // Run the model
        model.setNumIterations(2000);
        model.estimate();

        // File for topic keys
        File fileOutput = new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.TOPIC_KEYS_MALLET).toString());
        model.printTopWords(fileOutput, 20, false);

        // File for topic composition
        fileOutput = new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.TOPIC_COMPOSITION_MALLET).toString());
        model.printDocumentTopics(new PrintWriter(fileOutput));

        // Save model state and instances
        model.write(new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.DATA_MODEL_MALLET).toString()));
        instances.save(new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.DATA_MODEL_INSTANCES_MALLET).toString()));
    }

    /**
     * Tests a new document for topic modelling using mallet
     *
     * @param folderPath
     * @param document
     * @throws Exception
     */
    public void testTopicModellingUsingMallet(String folderPath, String document) throws Exception {
        ParallelTopicModel model = ParallelTopicModel.read(new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.DATA_MODEL_MALLET).toString()));
        InstanceList instances = InstanceList.load(new File(Paths.get(folderPath, Utils.TRAINING_FOLDER, Utils.DATA_MODEL_INSTANCES_MALLET).toString()));

        // Clean string
        document = Utils.cleanString(document, stopWordsStringPattern_, otherWordsStringPattern_, wordCountMap_);
        // Create a new instance with the document, empty target and source fields.
        InstanceList testing = new InstanceList(instances.getPipe());
        testing.addThruPipe(new Instance(document, null, "Test Instance", null));

        // Create the inferencer for new documents and get probabilities of topics
        TopicInferencer inferencer = model.getInferencer();
        double[] testProbabilities = inferencer.getSampledDistribution(testing.get(0), 30, 1, 5);

        // Create map where key is topic and value is probability, then sort by value.
        HashMap<Integer, Double> probabilitiesMap = new HashMap<Integer, Double>();
        for (int i = 0; i < testProbabilities.length; i++) {
            probabilitiesMap.put(i, testProbabilities[i]);
        }
        probabilitiesMap = (HashMap<Integer, Double>) Utils.sortByValueDesc(probabilitiesMap);
        System.out.println(probabilitiesMap);
    }

    public void generateFilePerTopic(String folderPath, String fileName, String splitChar, String pathAdviceCodesPerId)
            throws Exception {
        // TODO: modify for new files generated by mallet. Field 2 has id,X,word. It used to have the path of the file of the document.
        BufferedReader brInput = null;
        BufferedWriter bwOutputIdsPerTopic = null;
        try {
            String completePath = Paths.get(folderPath, fileName).toString();
            if (Utils.validateFile(completePath)) {

                // Map with topic number, and a map of advice code with its count
                HashMap<Integer, HashMap<String, Integer>> topicMapAdviceCodeLevel_1 = new HashMap<Integer, HashMap<String, Integer>>();
                HashMap<Integer, HashMap<String, Integer>> topicMapAdviceCodeLevel_2 = new HashMap<Integer, HashMap<String, Integer>>();
                HashMap<Integer, HashMap<String, Integer>> topicMapAdviceCodeLevel_3 = new HashMap<Integer, HashMap<String, Integer>>();
                HashMap<Integer, HashMap<String, Integer>> topicMapAdviceCodeAllLevels = new HashMap<Integer, HashMap<String, Integer>>();

                // File for ids with topic
                bwOutputIdsPerTopic = Utils.getBufferedWriter(folderPath, "topicperid.csv", true);

                brInput = Utils.getBufferedReader(completePath);
                String inputLine = brInput.readLine();
                // Read lines
                while (inputLine != null) {
                    // Do not process empty lines or lines beginning with #
                    if (inputLine.isEmpty() || inputLine.startsWith("#")) {
                        inputLine = brInput.readLine();
                        continue;
                    }

                    // Get file name
                    String[] values = inputLine.split(splitChar, -1);
                    String originalFilePath = values[1].trim().substring(6);

                    // Get id
                    String[] fileNameValues = originalFilePath.split("/");
                    String id = fileNameValues[fileNameValues.length - 1].replaceAll(".txt", "");

                    // Get file content
                    BufferedReader brOriginalInput = Utils.getBufferedReader(originalFilePath);
                    String originalFileContent = "";
                    String inputOriginalLine = brOriginalInput.readLine();
                    // Read lines
                    while (inputOriginalLine != null) {
                        // Do not process empty lines
                        if (inputOriginalLine.isEmpty()) {
                            inputOriginalLine = brOriginalInput.readLine();
                            continue;
                        }

                        originalFileContent += inputOriginalLine + " ";
                        inputOriginalLine = brOriginalInput.readLine();
                    }
                    brOriginalInput.close();

                    BigDecimal bestProbOfTopic = new BigDecimal(-1);
                    int topicNumber = -1;

                    // Get best topic with highest probability
                    for (int i = 2; i < values.length; i++) {
                        BigDecimal probOfTopic = new BigDecimal(values[i]);
                        if (probOfTopic.compareTo(bestProbOfTopic) > 0) {
                            bestProbOfTopic = probOfTopic;
                            topicNumber = i - 1;
                        }
                    }

                    // Append data to complete listing of ids with topic with highest probability
                    bwOutputIdsPerTopic.write(String.format("%s,%s", id, topicNumber));
                    bwOutputIdsPerTopic.newLine();

                    // Append data to file per topic
                    String fileNameTopic = topicNumber + ".txt";
                    String folderPathTopic = folderPath + "topics/";
                    Utils.createDirectory(folderPathTopic);
                    File fileTopic = new File(folderPathTopic + fileNameTopic);
                    BufferedWriter bwOutputTopic = new BufferedWriter(new FileWriter(fileTopic, true));
                    bwOutputTopic.write(originalFileContent);
                    bwOutputTopic.close();

                    // Add to advice code level 1
                    String adviceCodeLevel1 = idAdviceCodeLevel1Map_.get(id);
                    if (topicMapAdviceCodeLevel_1.containsKey(topicNumber)) {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_1.get(topicNumber);
                        if (adviceCodePerTopicMap.containsKey(adviceCodeLevel1))
                            countPerAdviceCode = adviceCodePerTopicMap.get(adviceCodeLevel1) + 1; // add 1 to advice code count

                        adviceCodePerTopicMap.put(adviceCodeLevel1, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_1.put(topicNumber, adviceCodePerTopicMap);
                    } else {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = new HashMap<String, Integer>();
                        adviceCodePerTopicMap.put(adviceCodeLevel1, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_1.put(topicNumber, adviceCodePerTopicMap);
                    }

                    // Add to advice code level 2
                    String adviceCodeLevel2 = idAdviceCodeLevel2Map_.get(id);
                    if (topicMapAdviceCodeLevel_2.containsKey(topicNumber)) {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_2.get(topicNumber);
                        if (adviceCodePerTopicMap.containsKey(adviceCodeLevel2))
                            countPerAdviceCode = adviceCodePerTopicMap.get(adviceCodeLevel2) + 1; // add 1 to advice code count

                        adviceCodePerTopicMap.put(adviceCodeLevel2, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_2.put(topicNumber, adviceCodePerTopicMap);
                    } else {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = new HashMap<String, Integer>();
                        adviceCodePerTopicMap.put(adviceCodeLevel2, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_2.put(topicNumber, adviceCodePerTopicMap);
                    }

                    // Add to advice code level 3
                    String adviceCodeLevel3 = idAdviceCodeLevel3Map_.get(id);
                    if (topicMapAdviceCodeLevel_3.containsKey(topicNumber)) {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_3.get(topicNumber);
                        if (adviceCodePerTopicMap.containsKey(adviceCodeLevel3))
                            countPerAdviceCode = adviceCodePerTopicMap.get(adviceCodeLevel3) + 1; // add 1 to advice code count

                        adviceCodePerTopicMap.put(adviceCodeLevel3, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_3.put(topicNumber, adviceCodePerTopicMap);
                    } else {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = new HashMap<String, Integer>();
                        adviceCodePerTopicMap.put(adviceCodeLevel3, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeLevel_3.put(topicNumber, adviceCodePerTopicMap);
                    }

                    // Add to advice code all levels
                    String adviceCodeAllLevels = String.format("%s|%s|%s", idAdviceCodeLevel1Map_.get(id),
                            idAdviceCodeLevel2Map_.get(id), idAdviceCodeLevel3Map_.get(id));
                    if (topicMapAdviceCodeAllLevels.containsKey(topicNumber)) {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeAllLevels.get(topicNumber);
                        if (adviceCodePerTopicMap.containsKey(adviceCodeAllLevels))
                            countPerAdviceCode = adviceCodePerTopicMap.get(adviceCodeAllLevels) + 1; // add 1 to advice code count

                        adviceCodePerTopicMap.put(adviceCodeAllLevels, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeAllLevels.put(topicNumber, adviceCodePerTopicMap);
                    } else {
                        int countPerAdviceCode = 1;
                        HashMap<String, Integer> adviceCodePerTopicMap = new HashMap<String, Integer>();
                        adviceCodePerTopicMap.put(adviceCodeAllLevels, countPerAdviceCode);

                        // add advice code count to topic
                        topicMapAdviceCodeAllLevels.put(topicNumber, adviceCodePerTopicMap);
                    }

                    inputLine = brInput.readLine();
                }

                // Create file for advice code per topic for Level 1
                for (Integer topicNumber : topicMapAdviceCodeLevel_1.keySet()) {

                    // Create file per topic with advice codes
                    String fileNameTopic = topicNumber + ".txt";
                    String folderPathTopic = folderPath + "topicsadvicecode1/";
                    Utils.createDirectory(folderPathTopic);
                    File fileTopic = new File(folderPathTopic + fileNameTopic);
                    BufferedWriter bwOutputTopic = new BufferedWriter(new FileWriter(fileTopic, true));

                    HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_1.get(topicNumber);

                    for (String advicecode : adviceCodePerTopicMap.keySet()) {

                        int countAdviceCode = adviceCodePerTopicMap.get(advicecode);
                        bwOutputTopic.write(String.format("%s;%d", advicecode, countAdviceCode));
                        bwOutputTopic.newLine();
                    }

                    bwOutputTopic.close();
                }

                // Create file for advice code per topic for Level 2
                for (Integer topicNumber : topicMapAdviceCodeLevel_2.keySet()) {

                    // Create file per topic with advice codes
                    String fileNameTopic = topicNumber + ".txt";
                    String folderPathTopic = folderPath + "topicsadvicecode2/";
                    Utils.createDirectory(folderPathTopic);
                    File fileTopic = new File(folderPathTopic + fileNameTopic);
                    BufferedWriter bwOutputTopic = new BufferedWriter(new FileWriter(fileTopic, true));

                    HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_2.get(topicNumber);

                    for (String advicecode : adviceCodePerTopicMap.keySet()) {

                        int countAdviceCode = adviceCodePerTopicMap.get(advicecode);
                        bwOutputTopic.write(String.format("%s;%d", advicecode, countAdviceCode));
                        bwOutputTopic.newLine();
                    }

                    bwOutputTopic.close();
                }

                // Create file for advice code per topic for Level 3
                for (Integer topicNumber : topicMapAdviceCodeLevel_3.keySet()) {

                    // Create file per topic with advice codes
                    String fileNameTopic = topicNumber + ".txt";
                    String folderPathTopic = folderPath + "topicsadvicecode3/";
                    Utils.createDirectory(folderPathTopic);
                    File fileTopic = new File(folderPathTopic + fileNameTopic);
                    BufferedWriter bwOutputTopic = new BufferedWriter(new FileWriter(fileTopic, true));

                    HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeLevel_3.get(topicNumber);

                    for (String advicecode : adviceCodePerTopicMap.keySet()) {

                        int countAdviceCode = adviceCodePerTopicMap.get(advicecode);
                        bwOutputTopic.write(String.format("%s;%d", advicecode, countAdviceCode));
                        bwOutputTopic.newLine();
                    }

                    bwOutputTopic.close();
                }

                // Create file for advice code per topic for all levels
                String fileNameTopic = "topicsadvicecode.csv";
                File fileTopic = new File(folderPath + fileNameTopic);
                BufferedWriter bwOutputTopic = new BufferedWriter(new FileWriter(fileTopic, true));
                for (Integer topicNumber : topicMapAdviceCodeAllLevels.keySet()) {
                    // Get advice codes by topic number
                    HashMap<String, Integer> adviceCodePerTopicMap = topicMapAdviceCodeAllLevels.get(topicNumber);
                    for (String advicecode : adviceCodePerTopicMap.keySet()) {
                        // Get advice code count
                        int countAdviceCode = adviceCodePerTopicMap.get(advicecode);
                        bwOutputTopic.write(String.format("%s;%s;%d", topicNumber, advicecode, countAdviceCode));
                        bwOutputTopic.newLine();
                    }
                }
                bwOutputTopic.close();
            } else
                System.err.println("File not found: " + completePath);
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                // Close files
                Utils.closeBufferedReader(brInput);
                Utils.closeBufferedWriter(bwOutputIdsPerTopic);
            } catch (Exception ex) {
                // Don't do anything
            }
        }
    }
}
