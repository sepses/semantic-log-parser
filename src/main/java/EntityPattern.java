import java.util.regex.Pattern;

/**
 * Class representation of EntityPattern
 */
public class EntityPattern {

    public String className;
    public String propertyName;
    public Boolean isObject;
    public Pattern pattern;
    public EnumPatternType type;

    public EntityPattern(String className, String propertyName, Boolean isObject, Pattern pattern,
            EnumPatternType type) {
        this.className = className;
        this.propertyName = propertyName;
        this.pattern = pattern;
        this.isObject = isObject;
        this.type = type;
    }
}
