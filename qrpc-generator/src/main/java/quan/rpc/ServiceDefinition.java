package quan.rpc;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ServiceDefinition {

    private int id;

    protected String name;

    //注释
    protected String comment;

    /**
     * 泛型的类型参数字符串
     */
    protected String typeParametersStr = "";

    /**
     * 泛型的类型参数边界，泛型参数名：类型上边界<T extends Object&Runnable>
     */
    protected LinkedHashMap<String, List<String>> typeParameterBounds = new LinkedHashMap<>();

    protected ServiceClassDefinition serviceClassDefinition;

    private Pattern simplifyableClassNamePattern;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String[] getComments() {
        if (comment == null) {
            return new String[0];
        } else {
            return comment.split("\n");
        }
    }

    public ServiceClassDefinition getServiceClass() {
        return serviceClassDefinition;
    }

    public String getTypeParametersStr() {
        return typeParametersStr;
    }

    public void setTypeParametersStr(String typeParametersStr) {
        this.typeParametersStr = typeParametersStr;
    }

    public LinkedHashMap<String, List<String>> getTypeParameterBounds() {
        return typeParameterBounds;
    }

    public void setTypeParameterBounds(LinkedHashMap<String, List<String>> typeParameterBounds) {
        this.typeParameterBounds = typeParameterBounds;
    }

    //有没有泛型参数
    public boolean hasTypeParameters() {
        return !typeParameterBounds.isEmpty();
    }

    public void prepare() {
        typeParametersStr = simplifyClassName(typeParametersStr);
    }

    /**
     * 简化类名
     *
     * @param className 包含泛型的全类名
     * @return 简化掉包名的类名
     */
    protected String simplifyClassName(String className) {
        if (StringUtils.isEmpty(className)) {
            return className;
        }

        if (simplifyableClassNamePattern == null) {
            //java.lang等包下的类不需要使用全类名
            List<String> defaultPackages = Arrays.asList("java.lang", "java.util", serviceClassDefinition.getPackageName());

            StringBuilder packagePatterns = new StringBuilder();
            for (String defaultPackage : defaultPackages) {
                String packagePattern = defaultPackage.replace(".", "\\.") + "\\.";
                if (packagePatterns.length() > 0) {
                    packagePatterns.append("|");
                }
                packagePatterns.append("(").append(packagePattern).append(")");
            }

            simplifyableClassNamePattern = Pattern.compile(String.format("(%s)[^.]+(,| |<|>|&|\\[|(\\.\\.\\.)|$)", packagePatterns));
        }

        Matcher matcher = simplifyableClassNamePattern.matcher(className);

        StringBuilder sb = new StringBuilder();

        int index = 0;
        while (matcher.find()) {
            sb.append(className, index, matcher.start(1));
            index = matcher.end(1);
        }

        sb.append(className, index, className.length());

        return sb.toString();

    }

}
