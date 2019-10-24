import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    static String NS = "http://w3id.org/sepses/vocab/log/core#";
    static String NS_INSTANCE = "http://w3id.org/sepses/id/";
    static String regexDomain = "((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}";
    static String regexURL = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    static String regexHost = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    static String regexUser = "(user|usr|ruser|uid|euid)(:|-|\\s)(\\w+)";
    static String regexPort = "(port)(:|-|\\s)(\\d+)";

    public static void main(String[] args) throws IOException {

        List<EntityPattern> patterns = new ArrayList<>();

        patterns.add(new EntityPattern("Url", Pattern.compile(regexURL), EnumPatternType.DataType));
        patterns.add(new EntityPattern("Host", Pattern.compile(regexHost), EnumPatternType.DataType));
        patterns.add(new EntityPattern("Domain", Pattern.compile(regexDomain), EnumPatternType.DataType));

        patterns.add(new EntityPattern("User", Pattern.compile(regexUser), EnumPatternType.Regex));
        patterns.add(new EntityPattern("Port", Pattern.compile(regexPort), EnumPatternType.Regex));

        Path path = Paths.get(".").toAbsolutePath().normalize();
        Path templatesPath = Paths.get(path.toString(), "//src//main//resources//OpenSSH_2k.log_templates.csv");
        Path dataPath = Paths.get(path.toString(), "//src//main//resources//OpenSSH_2k.log_structured.csv");

        Reader templatesIn = new FileReader(templatesPath.toFile());
        Reader dataIn = new FileReader(dataPath.toFile());

        Iterable<CSVRecord> templates = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(templatesIn);
        Iterable<CSVRecord> logLines = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(dataIn);

        List<LogLine> logLinesList = new ArrayList<>();
        for (CSVRecord logLine : logLines) {
            logLinesList
                    .add(new LogLine(logLine.get(0), logLine.get(1), logLine.get(2), logLine.get(3), logLine.get(4),
                            logLine.get(5), logLine.get(6), logLine.get(7), logLine.get(8), logLine.get(9)));
        }

        // get all templates plus occurrences of patterns plus properties
        List<Template> templatesList = AnnotateTemplates(patterns, templates, logLinesList);

        OntModel dataModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
        dataModel.read(path.toFile().getAbsolutePath() + "/src/main/resources/core.ttl", "Turtle");

        ParseLogLines(logLinesList, templatesList, dataModel);
    }

    /**
     * (1) first, look into logline that contain certain patterns by EventId=TemplateId
     * (2) next, take the parameter values and clean it up.
     *
     * @param patterns
     * @param templates
     * @param logLinesList
     * @return
     */
    private static List<Template> AnnotateTemplates(List<EntityPattern> patterns, Iterable<CSVRecord> templates,
            List<LogLine> logLinesList) {

        // Annotate template parameters
        List<Template> templatesList = new ArrayList<>();
        for (CSVRecord template : templates) {
            String templateText = template.get(1);
            Template temp = new Template(template.get(0), template.get(1));
            templatesList.add(temp);

            for (LogLine logline : logLinesList) {
                //LOG.info("Compare: " + logline.EventId + " : " + template.get(0));
                // (step 1) First, look into logline that contain certain patterns by EventId=TemplateId
                if (logline.EventId.equals(temp.TemplateId)) {
                    LOG.info("Found template example: " + logline.EventId + " - " + template.get(0) + ":"
                            + logline.Content);

                    // (step 2) next, take the parameter values and clean it up.
                    String cleanParameterValues =
                            logline.ParameterList.substring(1, logline.ParameterList.length() - 1)
                                    .replaceAll("'", "");
                    if (cleanParameterValues.isEmpty())
                        break;

                    String[] parameterValues = cleanParameterValues.split(",");
                    int c = -1;

                    for (String value : parameterValues) {
                        temp.parameterDict.add("");
                        c++;

                        // (step 3) split up OntClass pattern (datatype) and property value (regex)
                        for (EntityPattern pattern : patterns) {
                            if (pattern.type == EnumPatternType.DataType) {
                                // this is for parameter value
                                Matcher matcher = pattern.pattern.matcher(value);
                                if (matcher.find()) {
                                    LOG.info("Found datatype: " + value + " of " + pattern.className);
                                    temp.parameterDict.set(c, pattern.className);
                                }

                            }
                            else {
                                // this is for the whole logline
                                Matcher matcher = pattern.pattern.matcher(logline.Content);
                                if (matcher.find()) {
                                    LOG.info("Found pattern: " + value + " of " + pattern.className);
                                    if (matcher.group(0).contains(value)) {
                                        temp.parameterDict.set(c, pattern.className);
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        return templatesList;
    }

    private static void ParseLogLines(List<LogLine> logLinesList, List<Template> templatesList, OntModel dataModel) {

        OntClass logLineClass = dataModel.createClass(NS + "LogEntry");
        OntClass sourceClass = dataModel.createClass(NS + "Source");
        DatatypeProperty contentProperty = dataModel.createDatatypeProperty(NS + "content");
        DatatypeProperty dateProperty = dataModel.createDatatypeProperty(NS + "timestamp");
        DatatypeProperty levelProperty = dataModel.createDatatypeProperty(NS + "level");
        ObjectProperty sourceProperty = dataModel.createObjectProperty(NS + "hasSource");

        // Find entities in each line
        for (LogLine logline : logLinesList) {

            // Create instance for log line
            Individual lineInstance = logLineClass.createIndividual(NS_INSTANCE + logline.LineId);

            // Add basic properties of log line
            DatatypeProperty templateIdProperty = dataModel.createDatatypeProperty(NS + "templateId");
            lineInstance.addProperty(templateIdProperty, logline.EventId);
            lineInstance.addProperty(contentProperty, logline.Content);
            lineInstance.addProperty(dateProperty, logline.EventMonth + "_" + logline.EventDay + "_" + logline.EventTime);
            lineInstance.addProperty(levelProperty, logline.Level);

            // link to source
            Individual sourceInstance = sourceClass.createIndividual(NS_INSTANCE + logline.Component);
            lineInstance.addProperty(sourceProperty, sourceInstance);

            // get clean parameters
            String cleanParameterValues = logline.ParameterList.substring(1, logline.ParameterList.length() - 1).replaceAll("'", "");
            if (cleanParameterValues.isEmpty())
                continue;

            String[] parameterValues = cleanParameterValues.split(",");

            for (Template template : templatesList) {
                if (template.TemplateId.equals(logline.EventId)) {

                    // Extract variables
                    int c = 0;
                    for (String parameter : parameterValues) {
                        String targetType = template.parameterDict.get(c);
                        c++;
                        if(targetType.isEmpty())
                            continue;

                        LOG.info(String.format("Found: %s of Type %s", parameter, targetType));

                        Individual instance = dataModel.createClass(NS + targetType)
                                .createIndividual(NS_INSTANCE + parameter.replaceAll("[\\[\\.\\]\\s]", "_"));
                        ObjectProperty property = dataModel.createObjectProperty(NS + "connected" + targetType);
                        lineInstance.addProperty(property, instance);
                    }
                }
            }
        }

        try {
            FileWriter out = new FileWriter("Log_edited");// + DateTime.now().getMillis());
            dataModel.write(out, "Turtle");
        } catch (Exception e) {
            LOG.error("Error writing ontology: " + e.toString());
        }
    }
}
