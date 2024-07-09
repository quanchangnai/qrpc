package quan.rpc;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;

/**
 * 这个工具类能够解析出编译元素所在源文件名以及行号
 *
 * @author quanchangnai
 */
public class SourceLineResolver {

    public String resolveSourceLine(ProcessingEnvironment processingEnv, Element element) {
        return null;
    }

    public static SourceLineResolver newInstance() {
        try {
            //用到了jdk的私有类，有可能创建失败
            return new JavacSourceLineResolver();
        } catch (Throwable e) {
            return new SourceLineResolver();
        }
    }

}
