import java.util.ArrayList;
import java.util.List;

public class Template {
    public String TemplateId;
    public String TemplateContent;
    public List<String> parameterDict;

    public Template(String TemplateId, String TemplateContent){
        this.TemplateId = TemplateId;
        this.TemplateContent = TemplateContent;
        parameterDict = new ArrayList<String>();
    }
}