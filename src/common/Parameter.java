package common;

import org.apache.commons.cli.*;

/**
 * This class provides parameter to the entire application. It reads the command line
 * arguments and assigns the options to appropriate parameter. To avoid multiple instances
 * the singleton pattern is used.
 *
 * @author <a href="mailto:dhonsel@informatik.uni-goettingen.de">Daniel Honsel</a>
 * @author <a href="mailto:makedonski@informatik.uni-goettingen.de">Philip Makedonski</a>
 */
public class Parameter {
  protected static Parameter instance;
  protected String version = "0.10";
  private String toolname = "commonSHARK";

  // boolean parameter
  private boolean showVersion;
  private boolean showHelp;
  private boolean ssl;
  private boolean recordProgress;

  // database parameter
  private String dbName;
  private String dbUser;
  private String dbPassword;
  private String dbHostname;
  private int dbPort;
  private String dbAuthentication;

  private boolean separateDatabase;

  // debug parameter
  private String debugLevel;

  private boolean initialized = false;
  private OptionHandler optionsHandler;
  protected CommandLine cmd;

  public static synchronized Parameter getInstance() {
    if (instance == null) {
      instance = new Parameter();
      instance.setOptionsHandler(new OptionHandler());
    }
    return instance;
  }

  public void init(String[] args) {
    parseCommandLineArguments(args);
    checkArguments();
    
    showVersion = cmd.hasOption("v");
    showHelp = cmd.hasOption("h");
    ssl = cmd.hasOption("ssl");
	recordProgress = cmd.hasOption("RP");

    dbName = cmd.getOptionValue("DB", "smartshark");
    dbUser = cmd.getOptionValue("U", "");
    dbPassword = cmd.getOptionValue("P", "");
    dbHostname = cmd.getOptionValue("H", "localhost");
    dbPort = Integer.parseInt(cmd.getOptionValue("p", "27017"));
    dbAuthentication = cmd.getOptionValue("a", "");
    debugLevel = cmd.getOptionValue("d", "ERROR");

	separateDatabase = cmd.hasOption("sd");

    initialized = true;
  }


  public boolean isInitialized() {
    return initialized;
  }

  public String getDbName() {
    checkIfInitialised();
    return dbName;
  }

  public boolean isShowVersion() {
    checkIfInitialised();
    return showVersion;
  }

  public boolean isShowHelp() {
    checkIfInitialised();
    return showHelp;
  }

  public boolean isSsl() {
    checkIfInitialised();
    return ssl;
  }

  public String getDbUser() {
    checkIfInitialised();
    return dbUser;
  }

  public String getDbPassword() {
    checkIfInitialised();
    return dbPassword;
  }

  public String getDbHostname() {
    checkIfInitialised();
    return dbHostname;
  }

  public int getDbPort() {
    checkIfInitialised();
    return dbPort;
  }

  public String getDbAuthentication() {
    checkIfInitialised();
    return dbAuthentication;
  }

  protected void checkIfInitialised() {
	if (!isInitialized()) {
      System.out.println("The current parameter instance is not initialized!");
    }
  }

  public String getDebugLevel() {
    checkIfInitialised();
    return debugLevel;
  }

  private void parseCommandLineArguments(String[] args) {
    CommandLineParser parser =  new BasicParser();
    try {
      cmd = parser.parse(getOptionsHandler().getOptions(), args);
    } catch (ParseException e) {
      System.err.println("ERROR: " + e.getMessage());
      printHelp();
      System.exit(1);
    }
  }

  protected void checkArguments() {
	if (cmd.hasOption("h") ) {
        printHelp();
        System.exit(0);
    } else if (cmd.hasOption("v")) {
        printVersion();
        System.exit(0);
    }
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( getToolname(), getOptionsHandler().getOptions() );
  }

  public void printVersion() {
    System.out.println("This is " +getToolname()+ " version " + version);
  }

  public OptionHandler getOptionsHandler() {
	return optionsHandler;
  }

  public void setOptionsHandler(OptionHandler optionsHandler) {
	this.optionsHandler = optionsHandler;
  }

  public String getToolname() {
	return toolname;
  }

  public void setToolname(String toolname) {
	this.toolname = toolname;
  }

  public boolean isRecordProgress() {
	checkIfInitialised();
	return recordProgress;
  }

  public boolean isSeparateDatabase() {
    checkIfInitialised();
	return separateDatabase;
  }

}
