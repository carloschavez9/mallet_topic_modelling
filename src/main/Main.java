package main;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.nio.file.Paths;

public class Main {

    private static final String STOP_WORDS_PATH = "data/stopwords.txt";
    private static final String OTHER_WORDS_PATH = "data/otherwords.txt";
    private static final String DATA_ANALYSIS_FOLDER_PATH = "data_analysis";

    @Option(name = "-F", usage = "working folder")
    private String workingFolder_ = DATA_ANALYSIS_FOLDER_PATH;
    @Option(name = "-t", usage = "runs in training mode")
    private boolean trainingMode_ = false;
    @Option(name = "-f", usage = "training file name")
    private String trainingFile_ = "";
    @Option(name = "-n", usage = "number of topics to find")
    private int numTopics_ = 0;
    @Option(name = "-d", usage = "text of the document to test")
    private String document_ = "";

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            // parse the arguments
            parser.parseArgument(args);
            // Validate
            if (trainingMode_ && trainingFile_.isEmpty())
                throw new CmdLineException(parser, "Training file (-f) must be set in training mode", null);
            if (!trainingMode_ && document_.isEmpty())
                throw new CmdLineException(parser, "Document (-d) must be set for testing", null);
            if(trainingMode_ && numTopics_ <=0)
                throw new CmdLineException(parser, "Number of topics (-n) must be greater than 0 in training mode", null);

        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("java Main [options...]");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        try {
            if (trainingMode_) {
                train(trainingFile_, numTopics_);
            } else {
                test(document_);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Train the model for topic modelling using a file with all the documents and the number of topics to find
     *
     * @param trainingFileName File containing the documents to train the model with
     * @param numTopics        Number of topics to find
     * @throws Exception
     */
    private void train(String trainingFileName, int numTopics) throws Exception {
        System.out.println("Training started...");
        TopicModelling tfs = new TopicModelling(STOP_WORDS_PATH, OTHER_WORDS_PATH);
        // Load advice code ids
        tfs.loadAdviceCodeIds(Paths.get(workingFolder_, trainingFileName).toString(), ",", false);
        // Generate training file
        tfs.generateTrainingFile(workingFolder_, trainingFileName, ",", 0, 4, 5, 9, false, true, true, true);
        // Train the model
        tfs.trainTopicModellingUsingMallet(workingFolder_, Utils.DATA_TRAINING_FILENAME, numTopics);
        // Generate info per topic with advice codes
        //tfs.generateFilePerTopic(Utils.TRAINING_FOLDER, Utils.TOPIC_COMPOSITION_MALLET, ",", folderPath + fileName);
        System.out.println("Done");
    }

    /**
     * Test the model for topic modelling using a document
     *
     * @param document Document to test
     * @throws Exception
     */
    private void test(String document) throws Exception {
        System.out.println("Testing started...");
        TopicModelling tfs = new TopicModelling(STOP_WORDS_PATH, OTHER_WORDS_PATH);
        // Test the model using a document
        tfs.testTopicModellingUsingMallet(workingFolder_, document);
        System.out.println("Done");
    }

    /**
     * Main execution
     *
     * @param args Arguments
     */
    public static void main(String[] args) {
        new Main().doMain(args);
    }
}
