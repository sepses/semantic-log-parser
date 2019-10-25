import java.util.ArrayList;
import java.util.List;

/**
 * Class representation of a template
 */
public class Template {

    public String TemplateId;
    public String TemplateContent;
    public List<EntityPattern> parameterDict;

    public Template(String TemplateId, String TemplateContent) {
        this.TemplateId = TemplateId;
        this.TemplateContent = TemplateContent;
        parameterDict = new ArrayList<>();
    }
}
