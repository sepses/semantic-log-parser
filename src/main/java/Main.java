import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDFS;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
    private static String parserFilePath = "src/main/resources/parser.ttl";

    private static Map<String, String> templateIdMappings = new HashMap<>();

    /**
     * Main function - will be updated later to allow args parameterization
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        List<EntityPattern> patterns = new ArrayList<>();

        // check against parameters within parameter lists
        patterns.add(new EntityPattern("URL", "connectedURL", true, Pattern.compile(regexURL), EnumPatternType.Parameter));
        patterns.add(new EntityPattern("Host", "connectedHost", true, Pattern.compile(regexHost), EnumPatternType.Parameter));
        patterns.add(new EntityPattern("Domain", "connectedDomain", true, Pattern.compile(regexDomain), EnumPatternType.Parameter));

        // check against the entire log lines (context from parameter surroundings are needed)
        patterns.add(new EntityPattern("User", "connectedUser", true, Pattern.compile(regexUser), EnumPatternType.LogLine));
        patterns.add(new EntityPattern("Port", "port", false, Pattern.compile(regexPort), EnumPatternType.LogLine));

        Reader templateReader = new FileReader(Paths.get(logTemplate).toFile());
        Reader dataReader = new FileReader(Paths.get(logData).toFile());

        Iterable<CSVRecord> templates = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(templateReader);
        Iterable<CSVRecord> logLines = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(dataReader);

        List<LogLine> logLineList = new ArrayList<>();
        logLines.forEach(logLine -> logLineList.add(LogLine.fromOpenSSH(logLine)));

        OntModel dataModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM);
        InputStream is = Main.class.getClassLoader().getResourceAsStream(parserFilePath.substring(parserFilePath.lastIndexOf("/") + 1));
        RDFDataMgr.read(dataModel, is, Lang.TURTLE);

        List<Template> templatesList = new ArrayList<>();

        // load existing templates
        loadExistingTemplates(dataModel, templatesList);

        // get all templates plus occurrences of patterns plus properties
        try {
            annotateTemplates(patterns, templates, logLineList, dataModel, templatesList);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        parseLogLines(logLineList, templatesList, dataModel);
    }

    private static void loadExistingTemplates(OntModel dataModel, List<Template> templatesList) {
        OntClass extractedTemplateClass = dataModel.createClass(NS_PARSER + "ExtractedTemplate");
        OntClass extractedParamterClass = dataModel.createClass(NS_PARSER + "ExtractedParameter");

        DatatypeProperty contentProperty = dataModel.createDatatypeProperty(NS_PARSER + "content");
        DatatypeProperty hashProperty = dataModel.createDatatypeProperty(NS_PARSER + "hash");
        ObjectProperty hasParameterProperty = dataModel.createObjectProperty(NS_PARSER + "hasParameter");
        AnnotationProperty subjectProperty = dataModel.getAnnotationProperty("http://purl.org/dc/elements/1.1/subject");

        DatatypeProperty classNameProperty = dataModel.createDatatypeProperty(NS_PARSER + "className");
        DatatypeProperty propertyNameProperty = dataModel.createDatatypeProperty(NS_PARSER + "propertyName");
        DatatypeProperty patternProperty = dataModel.createDatatypeProperty(NS_PARSER + "pattern");
        DatatypeProperty isObjectProperty = dataModel.createDatatypeProperty(NS_PARSER + "isObject");
        DatatypeProperty positionProperty = dataModel.createDatatypeProperty(NS_PARSER + "position");
        DatatypeProperty typeProperty = dataModel.createDatatypeProperty(NS_PARSER + "type");

        ExtendedIterator templateIndividuals = extractedTemplateClass.listInstances();
        while (templateIndividuals.hasNext()) {
            Individual templateIndividual = (Individual) templateIndividuals.next();
            Template template = new Template(null, templateIndividual.getProperty(contentProperty).toString());
            template.hash = templateIndividual.getProperty(hashProperty).getString();

            Statement subj = templateIndividual.getProperty(subjectProperty);
            if (subj != null)
                template.subject = subj.getString();

            templatesList.add(template);

            StmtIterator hasChildStatementIterator = templateIndividual.listProperties(hasParameterProperty);
            while (hasChildStatementIterator.hasNext()) {
                Statement hasChildStatement = hasChildStatementIterator.next();
                EntityPattern parameter = new EntityPattern();

                Statement pos = hasChildStatement.getProperty(positionProperty);
                if (pos != null)
                    parameter.position = pos.getInt();

                // Try because the property might not exists - if it is just a placeholder parameter
                try {
                    Statement type = hasChildStatement.getProperty(typeProperty);
                    if (type != null)
                        parameter.type = EnumPatternType.valueOf(type.getString());
                } catch (Exception e) {
                }

                try {
                    Statement className = hasChildStatement.getProperty(classNameProperty);
                    if (className != null)
                        parameter.className = className.getString();
                } catch (Exception e) {
                }

                try {
                    Statement propertyName = hasChildStatement.getProperty(propertyNameProperty);
                    if (propertyName != null)
                        parameter.propertyName = propertyName.getString();
                } catch (Exception e) {
                }

                try {
                    Statement isObject = hasChildStatement.getProperty(isObjectProperty);
                    if (isObject != null)
                        parameter.isObject = isObject.getBoolean();
                } catch (Exception e) {
                }

                try {
                    Statement pattern = hasChildStatement.getProperty(patternProperty);
                    if (pattern != null)
                        parameter.pattern = Pattern.compile(pattern.getString());
                } catch (Exception e) {
                }

                template.parameterDict.add(parameter);
            }
        }
    }

    /**
     * (1) first, look into logline that contain certain patterns by EventId = TemplateId
     * (2) for each template, iterate the parameters and check which patterns are connected to the parameter.
     *
     * @param patterns
     * @param csvTemplates
     * @param logLineList
     * @param templatesList
     * @return
     */
    private static void annotateTemplates(List<EntityPattern> patterns, Iterable<CSVRecord> csvTemplates,
                                          List<LogLine> logLineList, OntModel dataModel, List<Template> templatesList) throws NoSuchAlgorithmException {

        boolean change = false;

        // Annotate template parameters
        for (CSVRecord csvTemplate : csvTemplates) {
            Template template = new Template(csvTemplate.get(0), csvTemplate.get(1));

            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hashbytes = digest.digest(template.TemplateContent.getBytes(StandardCharsets.UTF_8));
            String hash = new String(hashbytes);

            template.hash = hash;

            boolean exists = false;
            for (Template existingTemplate : templatesList) {
                if (existingTemplate.hash.equals(template.hash)) {
                    exists = true;

                    // Store mappings from templateId to Hash for this run
                    templateIdMappings.put(template.TemplateId, existingTemplate.hash);
                    break;
                }
            }

            if (exists)
                continue;

            change = true;

            for (LogLine logline : logLineList) {
                // look into logLine that contain certain patterns by EventId=TemplateId
                if (logline.EventId.equals(template.TemplateId)) {
                    LOG.info("Found template example: " + logline.EventId + ":" + logline.Content);
                    // process template parameters

                    processTemplateParameters(patterns, template, logline);

                    // Store templates in ontology if the template does not exist yet
                    createTemplateInstance(template, dataModel); // add it to the ontology for later
                    templatesList.add(template); // add it to the in memory list for this run
                    templateIdMappings.put(template.TemplateId, template.hash);

                    break; // after finding the first template patterns, don't need to go further
                }
            }
        }

        // Save templates to ontology
//        if (change) {
//            try {
//                FileWriter out = new FileWriter(parserFilePath);// + DateTime.now().getMillis());
//                dataModel.write(out, "Turtle");
//            } catch (Exception e) {
//                LOG.error("Error writing ontology: " + e.toString());
//            }
//        }
    }

    private static void createTemplateInstance(Template template, OntModel dataModel) {
        OntClass extractedTemplateClass = dataModel.createClass(NS_PARSER + "ExtractedTemplate");
        OntClass extractedParamterClass = dataModel.createClass(NS_PARSER + "ExtractedParameter");

        DatatypeProperty contentProperty = dataModel.createDatatypeProperty(NS_PARSER + "content");
        DatatypeProperty hashProperty = dataModel.createDatatypeProperty(NS_PARSER + "hash");
        ObjectProperty hasParameterProperty = dataModel.createObjectProperty(NS_PARSER + "hasParameter");
        DatatypeProperty positionProperty = dataModel.createDatatypeProperty(NS_PARSER + "position");

        DatatypeProperty classNameProperty = dataModel.createDatatypeProperty(NS_PARSER + "className");
        DatatypeProperty propertyNameProperty = dataModel.createDatatypeProperty(NS_PARSER + "propertyName");
        DatatypeProperty patternProperty = dataModel.createDatatypeProperty(NS_PARSER + "pattern");
        DatatypeProperty isObjectProperty = dataModel.createDatatypeProperty(NS_PARSER + "isObject");
        DatatypeProperty typeProperty = dataModel.createDatatypeProperty(NS_PARSER + "type");
        AnnotationProperty subjectProperty = dataModel.getAnnotationProperty("http://purl.org/dc/elements/1.1/subject");

        Individual templateIndividual = extractedTemplateClass.createIndividual(NS_INSTANCE + "Template_" + UUID.randomUUID());
        templateIndividual.addProperty(subjectProperty, "TestSubject");
        templateIndividual.addProperty(hashProperty, template.hash);
        templateIndividual.addProperty(contentProperty, template.TemplateContent);

        int pos = 0;
        for (EntityPattern param : template.parameterDict) {
            Individual paramIndividual = extractedParamterClass.createIndividual(NS_INSTANCE + "Parameter_" + UUID.randomUUID());
            paramIndividual.addProperty(positionProperty, String.valueOf(pos));
            templateIndividual.addProperty(hasParameterProperty, paramIndividual);
            pos++;

            if (param == null) // just a placeholder parameter for the position
                continue;

            paramIndividual.addProperty(typeProperty, param.type.toString());
            paramIndividual.addProperty(classNameProperty, param.className);
            paramIndividual.addProperty(propertyNameProperty, param.propertyName);
            paramIndividual.addProperty(patternProperty, param.pattern.toString());
            paramIndividual.addProperty(isObjectProperty, param.isObject.toString());
        }
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
     * <p>
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
                String loglineTemplateHash = templateIdMappings.get(logline.EventId); // Get hash from mapping
                if (template.hash.equals(loglineTemplateHash)) {
                    for (int counter = 0; counter < parameterValues.length; counter++) {
                        String parameter = parameterValues[counter].trim();
                        EntityPattern targetType = template.parameterDict.get(counter);
                        if (targetType == null || targetType.type == null) // Placeholder parameter (unknown) only has a position
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
            //FileWriter out = new FileWriter("log_KG.ttl");// + DateTime.now().getMillis());
            FileWriter out = new FileWriter(parserFilePath);
            dataModel.write(out, "Turtle");
        } catch (Exception e) {
            LOG.error("Error writing ontology: " + e.toString());
        }
    }

    /**
     * (1) take each log line and produce instance of LogEntry in the KG;
     * <p>
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

        // Create instance for log line - how to name it that it is unique enough?
        Individual lineInstance = logLineClass.createIndividual(NS_INSTANCE + "Logline_" + logline.LineId + "_SOURCE_" + logline.EventMonth + "_" + logline.EventDay + "_" + logline.EventTime);

        // Add basic properties of log line
        DatatypeProperty templateIdProperty = dataModel.createDatatypeProperty(NS_PARSER + "templateId");
        lineInstance.addProperty(templateIdProperty, logline.EventId);
        lineInstance.addProperty(contentProperty, logline.Content);
        try {
            lineInstance.addProperty(dateProperty, getDate(logline.EventMonth, logline.EventDay, logline.EventTime));
        } catch (ParseException e) {
            e.printStackTrace();
        }
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
    private static String getDate(String month, String day, String time) throws ParseException {

        day = StringUtils.leftPad(day, 2, "0");

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, DateTime.now().getYear());
        cal.set(Calendar.MONTH, new SimpleDateFormat("MMM", Locale.ENGLISH).parse(month).getMonth());
        cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
        Date dateRepresentation = cal.getTime();

        SimpleDateFormat xmlDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String dateString;

        dateString = xmlDateFormatter.format(dateRepresentation);

        return dateString;
    }
}
