import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.List;

public class EntityPattern {
    public Pattern pattern;
    public String className;
    public EnumPatternType type;

    public EntityPattern(String className, Pattern pattern, EnumPatternType type){
        this.className = className;
        this.pattern = pattern;
        this.type = type;
    }
}
