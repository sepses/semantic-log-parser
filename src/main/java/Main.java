import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static String NS_CORE = "http://w3id.org/sepses/vocab/log/core#";
    static String NS_PARSER = "http://w3id.org/sepses/vocab/log/parser#";
    static String NS_INSTANCE = "http://w3id.org/sepses/id/";

    static String regexDomain = "((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}";
    static String regexURL = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    static String regexHost = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";

    static String regexUser = "(user|usr|ruser|uid|euid)(:|-|\\s)(\\w+)";
    static String regexPort = "(port)(:|-|\\s)(\\d+)";

    // TODO: will be replaced by JAVA args later on
    private static String logTemplate = "./input/OpenSSH_2k.log_templates.csv";
    private static String logData = "./input/OpenSSH_2k.log_structured.csv";

    /**
     * Main function - will be updated later to allow args parameterization
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        List<EntityPattern> patterns = new ArrayList<>();

        // check against parameters within parameter lists
        patterns.add(
                new EntityPattern("URL", "connectedURL", true, Pattern.compile(regexURL), EnumPatternType.Parameter));
        patterns.add(new EntityPattern("Host", "connectedHost", true, Pattern.compile(regexHost),
                EnumPatternType.Parameter));
        patterns.add(new EntityPattern("Domain", "connectedDomain", true, Pattern.compile(regexDomain),
                EnumPatternType.Parameter));

        // check against the entire log lines (context from parameter surroundings are needed)
        patterns.add(new EntityPattern("User", "connectedUser", true, Pattern.compile(regexUser),
                EnumPatternType.LogLine));
        patterns.add(new EntityPattern("Port", "port", false, Pattern.compile(regexPort), EnumPatternType.LogLine));

        Reader templateReader = new FileReader(Paths.get(logTemplate).toFile());
        Reader dataReader = new FileReader(Paths.get(logData).toFile());

        Iterable<CSVRecord> templates = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(templateReader);
        Iterable<CSVRecord> logLines = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(dataReader);

        List<LogLine> logLineList = new ArrayList<>();
        logLines.forEach(logLine -> logLineList.add(LogLine.fromOpenSSH(logLine)));

        // get all templates plus occurrences of patterns plus properties
        List<Template> templatesList = annotateTemplates(patterns, templates, logLineList);

        OntModel dataModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);
        InputStream is = Main.class.getClassLoader().getResourceAsStream("parser.ttl");
        RDFDataMgr.read(dataModel, is, Lang.TURTLE);

        parseLogLines(logLineList, templatesList, dataModel);

    }

    /**
     * (1) first, look into logline that contain certain patterns by EventId = TemplateId
     * (2) for each template, iterate the parameters and check which patterns are connected to the parameter.
     *
     * @param patterns
     * @param csvTemplates
     * @param logLineList
     * @return
     */
    private static List<Template> annotateTemplates(List<EntityPattern> patterns, Iterable<CSVRecord> csvTemplates,
            List<LogLine> logLineList) {

        // Annotate template parameters
        List<Template> templateList = new ArrayList<>();
        for (CSVRecord csvTemplate : csvTemplates) {

            Template template = new Template(csvTemplate.get(0), csvTemplate.get(1));
            templateList.add(template);

            for (LogLine logline : logLineList) {
                // look into logLine that contain certain patterns by EventId=TemplateId
                if (logline.EventId.equals(template.TemplateId)) {
                    LOG.info("Found template example: " + logline.EventId + ":" + logline.Content);
                    // process template parameters

                    processTemplateParameters(patterns, template, logline);
                    break; // after finding the first template patterns, don't need to go further
                }
            }
        }
        return templateList;
    }

    /**
     * Check which regex patterns are related to template parameters and record it in the Template class instance
     *
     * @param patterns
     * @param template
     * @param logLine
     */
    private static void processTemplateParameters(List<EntityPattern> patterns, Template template, LogLine logLine) {

        // take the parameter values and clean it up.
        String paramValues = logLine.ParameterList.substring(1, logLine.ParameterList.length() - 1);
        paramValues = paramValues.replaceAll("'", "");

        // if not empty, continue
        if (!paramValues.isEmpty()) {
            String[] paramValuesArray = paramValues.split(",");
            for (String value : paramValuesArray) {
                EntityPattern foundPattern = null;
                // depending on type of pattern, process it differently
                for (EntityPattern pattern : patterns) {
                    if (pattern.type == EnumPatternType.Parameter) {
                        // the pattern match property values exactly
                        Matcher matcher = pattern.pattern.matcher(value);
                        if (matcher.find()) {
                            LOG.info("Found Parameter Regex: " + value + " of " + pattern.className);
                            foundPattern = pattern;
                            break;
                        }
                    } else if (pattern.type == EnumPatternType.LogLine) {
                        // the pattern requires "context" information from its surrounding
                        Matcher matcher = pattern.pattern.matcher(logLine.Content);
                        if (matcher.find()) {
                            String tempValue = matcher.group(0);
                            if (tempValue.contains(value)) {
                                LOG.info("Found Content Regex: " + value + " of " + pattern.className);
                                foundPattern = pattern;
                            }
                        }
                    } else {
                        // pattern type is not recognized
                        LOG.error("Found unrecognized pattern type: " + pattern.type);
                        template.parameterDict.add(null);
                    }
                }

                if (foundPattern == null) {
                    // pattern not found for certain values
                    LOG.warn("Value '" + value + "' doesn't match any patterns");
                }
                template.parameterDict.add(foundPattern);
            }
        }
    }

    /**
     * (1) take each log line and produce instance of LogEntry in the KG;
     *
     * (2) based on the template parameters, add additional information on URL, HOST, USER, DOMAIN, and PORT
     *
     * @param logLines
     * @param templatesList
     * @param dataModel
     */
    private static void parseLogLines(List<LogLine> logLines, List<Template> templatesList, OntModel dataModel) {

        // Find entities in each line
        for (LogLine logline : logLines) {
            LOG.info("Process logline-" + logline.LineId);

            // create individual for each log line
            Individual lineInstance = getLineInstance(dataModel, logline);

            // get clean parameters
            String paramValues = logline.ParameterList.substring(1, logline.ParameterList.length() - 1);
            paramValues = paramValues.replaceAll("'", "");

            // process parameter values
            if (paramValues.isEmpty())
                continue; // if empty, skip this log line

            String[] parameterValues = paramValues.split(",");
            for (Template template : templatesList) {
                if (template.TemplateId.equals(logline.EventId)) {
                    for (int counter = 0; counter < parameterValues.length; counter++) {
                        String parameter = parameterValues[counter].trim();
                        EntityPattern targetType = template.parameterDict.get(counter);
                        if (targetType == null)
                            continue; // if null, skip

                        LOG.info(String.format("Found: %s of Type %s", parameter, targetType.className));
                        if (targetType.isObject) {
                            OntClass ontClass = dataModel.createClass(NS_CORE + targetType.className);

                            Individual instance = ontClass.createIndividual(
                                    NS_INSTANCE + targetType.className + "_" + parameter.trim()
                                            .replaceAll("[\\[\\.\\]\\s]", "_"));
                            instance.addProperty(RDFS.label, parameter);

                            ObjectProperty property =
                                    dataModel.createObjectProperty(NS_PARSER + targetType.propertyName);
                            lineInstance.addProperty(property, instance);
                        } else {
                            DatatypeProperty property =
                                    dataModel.createDatatypeProperty(NS_PARSER + targetType.propertyName);
                            lineInstance.addProperty(property, parameter);
                        }
                    }
                }
            }
        }

        try {
            FileWriter out = new FileWriter("log_KG.ttl");// + DateTime.now().getMillis());
            dataModel.write(out, "Turtle");
        } catch (Exception e) {
            LOG.error("Error writing ontology: " + e.toString());
        }
    }

    /**
     * (1) take each log line and produce instance of LogEntry in the KG;
     *
     * (2) add basic information into the LogEntry resource
     *
     * @param dataModel
     * @param logline
     * @return
     */
    private static Individual getLineInstance(OntModel dataModel, LogLine logline) {

        OntClass logLineClass = dataModel.createClass(NS_CORE + "LogEntry");
        OntClass sourceClass = dataModel.createClass(NS_PARSER + "Source");

        DatatypeProperty contentProperty = dataModel.createDatatypeProperty(NS_CORE + "logMessage");
        DatatypeProperty dateProperty = dataModel.createDatatypeProperty(NS_CORE + "timestamp");
        DatatypeProperty levelProperty = dataModel.createDatatypeProperty(NS_CORE + "level");
        ObjectProperty sourceProperty = dataModel.createObjectProperty(NS_PARSER + "hasSource");
        DatatypeProperty sequenceProperty = dataModel.createDatatypeProperty(NS_PARSER + "sequence");

        // Create instance for log line
        Individual lineInstance = logLineClass.createIndividual(NS_INSTANCE + logline.LineId);

        // Add basic properties of log line
        DatatypeProperty templateIdProperty = dataModel.createDatatypeProperty(NS_PARSER + "templateId");
        lineInstance.addProperty(templateIdProperty, logline.EventId);
        lineInstance.addProperty(contentProperty, logline.Content);
        lineInstance.addProperty(dateProperty, getDate(logline.EventMonth, logline.EventDay, logline.EventTime));
        lineInstance.addProperty(levelProperty, logline.Level);
        lineInstance.addLiteral(sequenceProperty, logline.LineId);

        // link to source
        Individual sourceInstance = sourceClass.createIndividual(NS_INSTANCE + logline.Component);
        lineInstance.addProperty(sourceProperty, sourceInstance);

        return lineInstance;
    }

    /**
     * Create a correctly formatted date from inputs
     *
     * @param month
     * @param day
     * @param time
     * @return Date
     */
    private static String getDate(String month, String day, String time) {

        day = StringUtils.leftPad(day, 2, "0");
        StringBuilder sb = new StringBuilder();
        sb.append(DateTime.now().getYear()).append(" ").append(month).append(" ").append(day).append(" ")
                .append(time);
        String dateString = sb.toString();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
        SimpleDateFormat xmlDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            Date date = formatter.parse(dateString);
            dateString = xmlDateFormatter.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return dateString;
    }
}
