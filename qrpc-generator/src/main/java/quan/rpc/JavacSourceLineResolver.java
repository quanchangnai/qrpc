package quan.rpc;

import com.sun.source.tree.CompilationUnitTree;
import com.sun.tools.javac.api.JavacTrees;
import com.sun.tools.javac.file.PathFileObject;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;

/**
 * @author quanchangnai
 */
public class JavacSourceLineResolver extends SourceLineResolver {

    /**
     * 获取元素所在源文件名以及行号
     * 参考 jdk.javadoc.internal.tool.Messager#getDiagSource(Element)
     */
    @Override
    public String resolveSourceLine(ProcessingEnvironment processingEnv, Element element) {
        JavacTrees javacTrees = JavacTrees.instance(processingEnv);
        CompilationUnitTree compilationUnit = javacTrees.getPath(element).getCompilationUnit();
        long sourcePosition = javacTrees.getSourcePositions().getStartPosition(compilationUnit, javacTrees.getTree(element));
        long lineNumber = compilationUnit.getLineMap().getLineNumber(sourcePosition);
        String fileName = PathFileObject.getSimpleName(compilationUnit.getSourceFile());
        return fileName + ":" + lineNumber;
    }

}
