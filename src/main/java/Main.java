import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    static String NS = "http://www.semanticweb.org/sepses/logs#";
    static String regexDomain = "((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}";
    static String regexURL = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    static String regexHost = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    static String regexUser = "(user|usr|ruser|uid|euid)(:|-|\\s)(\\w+)";
    static String regexPort = "(port)(:|-|\\s)(\\d+)";

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        List<EntityPattern> patterns = new ArrayList<>();
        patterns.add(new EntityPattern("Url", Pattern.compile(regexURL), EnumPatternType.DataType));
        patterns.add(new EntityPattern("Host", Pattern.compile(regexHost), EnumPatternType.DataType));
        patterns.add(new EntityPattern("User", Pattern.compile(regexUser), EnumPatternType.Regex));
        patterns.add(new EntityPattern("Port", Pattern.compile(regexPort), EnumPatternType.Regex));
        patterns.add(new EntityPattern("Domain", Pattern.compile(regexDomain), EnumPatternType.DataType));

        Path path = Paths.get(".").toAbsolutePath().normalize();
        Path templatesPath = Paths.get(path.toString(), "//src//main//resources//OpenSSH_2k.log_templates.csv");
        Path dataPath = Paths.get(path.toString(), "//src//main//resources//OpenSSH_2k.log_structured.csv");

        Reader templatesIn = new FileReader(templatesPath.toFile());
        Reader dataIn = new FileReader(dataPath.toFile());

        Iterable<CSVRecord> templates = CSVFormat.DEFAULT
                .withFirstRecordAsHeader().parse(templatesIn);

        Iterable<CSVRecord> logLines = CSVFormat.DEFAULT
                .withFirstRecordAsHeader().parse(dataIn);

        List<LogLine> logLinesList = new ArrayList<>();
        for (CSVRecord logline : logLines) {
            logLinesList.add(new LogLine(logline.get(0), logline.get(1), logline.get(2), logline.get(3), logline.get(4), logline.get(5), logline.get(6), logline.get(7), logline.get(8), logline.get(9)));
        }

        List<Template> templatesList = AnnotateTemplates(patterns, templates, logLinesList);

        OntModel dataModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
        dataModel.read(path.toFile().getAbsolutePath() + "/src/main/resources/LogOntology.ttl", "Turtle");

        ParseLogLines(logLinesList, templatesList, dataModel);
    }

    private static List<Template> AnnotateTemplates(List<EntityPattern> patterns, Iterable<CSVRecord> templates, List<LogLine> logLinesList) {
        // Annotate template parameters
        List<Template> templatesList = new ArrayList<>();
        for (CSVRecord template : templates) {
            String templateText = template.get(1);
            Template temp = new Template(template.get(0), template.get(1));
            templatesList.add(temp);

            for (LogLine logline : logLinesList) {
                //LOG.info("Compare: " + logline.EventId + " : " + template.get(0));
                if (logline.EventId.equals(temp.TemplateId)) {
                    LOG.info("Found template example: " + logline.EventId + " - " + template.get(0) + ":" + logline.Content);
                    String cleanParameterValues = logline.ParameterList.substring(1, logline.ParameterList.length() - 1).replaceAll("'", "");
                    if(cleanParameterValues.isEmpty())
                        break;

                    String[] parameterValues = cleanParameterValues.split(",");

                    int c = -1;
                    for (String value : parameterValues) {
                        temp.parameterDict.add("");
                        c++;

                        for (EntityPattern pattern : patterns) {
                            if (pattern.type == EnumPatternType.DataType) {
                                Matcher matcher = pattern.pattern.matcher(value);
                                if (matcher.find()) {
                                    LOG.info("Found datatype: " + value + " of " + pattern.className);
                                    temp.parameterDict.set(c, pattern.className);
                                }
                            } else {
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
        OntClass logLineClass = dataModel.getOntClass(NS + "LogLine");
        OntClass sourceClass = dataModel.getOntClass(NS + "Source");
        DatatypeProperty contentProperty = dataModel.getDatatypeProperty( NS + "content" );
        DatatypeProperty dateProperty = dataModel.getDatatypeProperty( NS + "date" );
        DatatypeProperty levelProperty = dataModel.getDatatypeProperty( NS + "level" );
        ObjectProperty sourceProperty = dataModel.getObjectProperty( NS + "hasSource" );

        // Find entities in each line
        for (LogLine logline : logLinesList) {
            // Create instance for log line
            Individual lineInstance = createIndividual(dataModel, logline.LineId, logLineClass);
            // Add basic properties
            DatatypeProperty templateIdProperty = dataModel.getDatatypeProperty( NS + "templateId" );
            lineInstance.addProperty(templateIdProperty, logline.EventId);
            lineInstance.addProperty(contentProperty, logline.Content);
            lineInstance.addProperty(dateProperty, logline.EventMonth + "_" + logline.EventDay + "_" + logline.EventTime);
            lineInstance.addProperty(levelProperty, logline.Level);

            Individual sourceInstance = createIndividual(dataModel, logline.Component, sourceClass );
            lineInstance.addProperty(sourceProperty, sourceInstance);

            String cleanParameterValues = logline.ParameterList.substring(1, logline.ParameterList.length() - 1).replaceAll("'", "");
            if(cleanParameterValues.isEmpty())
                continue;

            String[] parameterValues = cleanParameterValues.split(",");

            for (Template template : templatesList) {
                if (template.TemplateId.equals(logline.EventId)) {
                    // Extract variables
                    int c = 0;
                    for (String parameter : parameterValues) {
                        String targetType = template.parameterDict.get(c);

                        if(targetType.isEmpty())
                            continue;

                        LOG.info(String.format("Found: %s ofType %s", parameter, targetType));

                        // Add property
                        Individual instance = createIndividual(dataModel, parameter.replaceAll("[\\[\\.\\]\\s]", "_"), dataModel.getOntClass(NS + targetType) );
                        ObjectProperty property = dataModel.getObjectProperty( NS + "connected" + targetType );
                        lineInstance.addProperty(property, instance);

                        c++;
                    }
                }
            }
        }

        try {
            FileWriter out = new FileWriter("Log_edited");// + DateTime.now().getMillis());
            dataModel.write(out, "Turtle");
        }catch(Exception e){
            LOG.error("Error writing ontology: " + e.toString());
        }
    }

    private static Individual createIndividual(OntModel dataModel, String name, OntClass logLineClass) {
        return dataModel.createIndividual(NS + logLineClass + "_" + name, logLineClass);
    }
}