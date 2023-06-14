package quan.rpc;

import freemarker.template.Configuration;
import freemarker.template.Template;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Pattern;

@SupportedOptions("rpcProxyPath")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes({"quan.rpc.Endpoint", "quan.rpc.Service.Single"})
public class Generator extends AbstractProcessor {

    private Messager messager;

    private Filer filer;

    private Types typeUtils;

    private Elements elementUtils;

    private TypeMirror serviceType;

    private TypeMirror promiseType;

    /**
     * 自定义代理类的生成路径
     */
    private String proxyPath;

    private final Pattern illegalMethodPattern = Pattern.compile("_.*\\$");

    private final List<Modifier> illegalMethodModifiers = Arrays.asList(Modifier.PRIVATE, Modifier.STATIC);

    private Template proxyTemplate;

    private Template callerTemplate;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        messager = processingEnv.getMessager();
        filer = processingEnv.getFiler();
        typeUtils = processingEnv.getTypeUtils();
        elementUtils = processingEnv.getElementUtils();

        serviceType = elementUtils.getTypeElement(Service.class.getName()).asType();
        promiseType = typeUtils.erasure(elementUtils.getTypeElement(Promise.class.getName()).asType());
        proxyPath = processingEnv.getOptions().get("rpcProxyPath");

        try {
            Configuration freemarkerCfg = new Configuration(Configuration.VERSION_2_3_23);
            freemarkerCfg.setClassForTemplateLoading(getClass(), "");
            freemarkerCfg.setDefaultEncoding("UTF-8");
            proxyTemplate = freemarkerCfg.getTemplate("proxy.ftl");
            callerTemplate = freemarkerCfg.getTemplate("caller.ftl");
        } catch (IOException e) {
            printError(e);
        }
    }

    private void printError(String msg, Element element) {
        messager.printMessage(Diagnostic.Kind.ERROR, msg, element);
    }

    private void printError(Exception e) {
        messager.printMessage(Diagnostic.Kind.ERROR, e.toString());
        e.printStackTrace();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<TypeElement> typeElements = new HashSet<>();

        for (TypeElement annotation : annotations) {
            boolean endpoint = annotation.getQualifiedName().contentEquals(Endpoint.class.getName());
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (endpoint) {
                    typeElements.add((TypeElement) element.getEnclosingElement());
                } else {
                    typeElements.add((TypeElement) element);
                }
            }
        }

        for (Element rootElement : roundEnv.getRootElements()) {
            if (rootElement instanceof TypeElement && typeUtils.isSubtype(rootElement.asType(), serviceType)) {
                //没有加注解的服务类
                typeElements.add((TypeElement) rootElement);
            }
        }

        for (TypeElement typeElement : typeElements) {
            processServiceClass(typeElement);
        }

        return true;
    }

    private boolean checkServiceClass(TypeElement typeElement) {
        boolean success = true;

        boolean isSingle = typeElement.getAnnotation(Service.Single.class) != null;
        boolean isServiceType = typeUtils.isSubtype(typeElement.asType(), serviceType);

        Set<ExecutableElement> executableElements = new LinkedHashSet<>();
        for (Element enclosedElement : typeElement.getEnclosedElements()) {
            if (enclosedElement instanceof ExecutableElement) {
                executableElements.add((ExecutableElement) enclosedElement);
            }
        }

        if (typeElement.getModifiers().contains(Modifier.ABSTRACT)) {
            success = false;
        }

        if (isServiceType && typeElement.getNestingKind().isNested()) {
            printError("service class cannot is nested", typeElement);
        }

        if (isSingle) {
            if (isServiceType) {
                for (ExecutableElement executableElement : executableElements) {
                    if (executableElement.getSimpleName().contentEquals("getId") && executableElement.getParameters().isEmpty()) {
                        success = false;
                        printError("single service class cannot override getId method", executableElement);
                        break;
                    }
                }
            } else if (typeElement.getKind() != ElementKind.CLASS) {
                success = false;
                printError(typeElement.getKind().name().toLowerCase() + "  cannot declare a " + Service.Single.class.getName() + " annotation", typeElement);
            } else {
                success = false;
                printError("non service class cannot declare a " + Service.Single.class.getName() + " annotation", typeElement);
            }
        }

        for (ExecutableElement executableElement : executableElements) {
            if (illegalMethodPattern.matcher(executableElement.getSimpleName()).matches()) {
                success = false;
                printError("the name of the method is illegal", executableElement);
            }

            if (!isServiceType) {
                success = false;
                printError("endpoint method cannot declare in non service class", executableElement);
            } else {
                for (Modifier illegalModifier : illegalMethodModifiers) {
                    if (executableElement.getModifiers().contains(illegalModifier)) {
                        success = false;
                        printError("endpoint method cant not declare one of " + illegalMethodModifiers, executableElement);
                    }
                }
            }
        }

        return success;
    }

    private void processServiceClass(TypeElement typeElement) {
        if (!checkServiceClass(typeElement)) {
            return;
        }

        Set<ExecutableElement> executableElements = new LinkedHashSet<>();
        for (Element memberElement : elementUtils.getAllMembers(typeElement)) {
            if (memberElement instanceof ExecutableElement && memberElement.getAnnotation(Endpoint.class) != null) {
                executableElements.add((ExecutableElement) memberElement);
            }
        }

        ServiceClass serviceClass = new ServiceClass(typeElement.getQualifiedName().toString());
        serviceClass.setComment(elementUtils.getDocComment(typeElement));
        serviceClass.setOriginalTypeParameters(processTypeParameters(typeElement.getTypeParameters()));

        Service.Single single = typeElement.getAnnotation(Service.Single.class);
        if (single != null) {
            serviceClass.setServiceId(single.id());
        }

        for (ExecutableElement executableElement : executableElements) {
            ServiceMethod serviceMethod = processServiceMethod(executableElement);
            serviceMethod.setServiceClass(serviceClass);
            serviceClass.getMethods().add(serviceMethod);
        }

        try {
            generateProxy(serviceClass);
            generateCaller(serviceClass);
        } catch (IOException e) {
            printError(e);
        }
    }

    private LinkedHashMap<String, List<String>> processTypeParameters(List<? extends TypeParameterElement> typeParameterElements) {
        LinkedHashMap<String, List<String>> typeParameters = new LinkedHashMap<>();

        for (TypeParameterElement typeParameter : typeParameterElements) {
            List<String> typeBounds = new ArrayList<>();
            for (TypeMirror typeBound : typeParameter.getBounds()) {
                typeBounds.add(typeBound.toString());
            }
            typeParameters.put(typeParameter.getSimpleName().toString(), typeBounds);
        }

        return typeParameters;
    }

    private ServiceMethod processServiceMethod(ExecutableElement executableElement) {
        ServiceMethod serviceMethod = new ServiceMethod(executableElement.getSimpleName());
        serviceMethod.setComment(elementUtils.getDocComment(executableElement));
        serviceMethod.setOriginalTypeParameters(processTypeParameters(executableElement.getTypeParameters()));
        serviceMethod.setVarArgs(executableElement.isVarArgs());

        Endpoint endpoint = executableElement.getAnnotation(Endpoint.class);
        boolean paramSafe = true;

        for (VariableElement parameter : executableElement.getParameters()) {
            TypeMirror parameterType = parameter.asType();
            serviceMethod.addParameter(parameter.getSimpleName(), parameterType.toString());
            if (!ConstantUtils.isConstantType(parameterType)) {
                paramSafe = false;
            }
        }

        TypeMirror returnType = executableElement.getReturnType();

        if (returnType.getKind().isPrimitive()) {
            serviceMethod.setOriginalReturnType(typeUtils.boxedClass((PrimitiveType) returnType).asType().toString());
        } else if (returnType.getKind() == TypeKind.VOID) {
            serviceMethod.setOriginalReturnType(Void.class.getSimpleName());
        } else if (typeUtils.isSubtype(typeUtils.erasure(returnType), promiseType)) {
            serviceMethod.setOriginalReturnType(((DeclaredType) returnType).getTypeArguments().get(0).toString());
        } else {
            serviceMethod.setOriginalReturnType(returnType.toString());
        }

        if (!paramSafe) {
            paramSafe = endpoint.paramSafe();
        }

        boolean resultSafe = ConstantUtils.isConstantType(returnType);
        if (!resultSafe) {
            resultSafe = endpoint.resultSafe();
        }

        serviceMethod.setParamSafe(paramSafe);
        serviceMethod.setResultSafe(resultSafe);

        return serviceMethod;
    }

    /**
     * 自定义递归创建目录，因为使用gradle编译时，File.mkdirs的路径不对
     */
    private boolean mkdirs(File path) {
        if (path.exists()) {
            return false;
        }
        if (path.mkdir()) {
            return true;
        }
        File parent = path.getParentFile();
        return parent != null && mkdirs(parent);
    }

    private void generateProxy(ServiceClass serviceClass) throws IOException {
        serviceClass.setCustomPath(proxyPath != null);
        serviceClass.optimizeImport4Proxy();
        Writer proxyWriter;

        if (proxyPath == null) {
            JavaFileObject proxyFile = filer.createSourceFile(serviceClass.getFullName() + "Proxy");
            proxyWriter = proxyFile.openWriter();
        } else {
            File path = new File(proxyPath, serviceClass.getPackageName().replace(".", "/"));
            mkdirs(path);
            File file = new File(path, serviceClass.getName() + "Proxy.java");
            proxyWriter = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8);
        }

        try {
            proxyTemplate.process(serviceClass, proxyWriter);
        } catch (Exception e) {
            printError(e);
        } finally {
            proxyWriter.close();
        }

    }

    private void generateCaller(ServiceClass serviceClass) throws IOException {
        serviceClass.setCustomPath(false);
        serviceClass.optimizeImport4Caller();
        JavaFileObject callerFile = filer.createSourceFile(serviceClass.getFullName() + "Caller");

        try (Writer callerWriter = callerFile.openWriter()) {
            callerTemplate.process(serviceClass, callerWriter);
        } catch (Exception e) {
            printError(e);
        }
    }

}
