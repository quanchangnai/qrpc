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
import java.util.stream.Collectors;

@SupportedOptions("rpcProxyPath")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes({"quan.rpc.Endpoint", "quan.rpc.ProxyConstructors"})
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

    private final Pattern illegalMethodNamePattern = Pattern.compile("_.*\\$");

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
        serviceType = typeUtils.erasure(elementUtils.getTypeElement(Service.class.getName()).asType());
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
        Set<TypeElement> classElements = new HashSet<>();

        for (TypeElement annotation : annotations) {
            boolean endpoint = annotation.getQualifiedName().contentEquals(Endpoint.class.getName());
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (endpoint) {
                    classElements.add((TypeElement) element.getEnclosingElement());
                } else {
                    classElements.add((TypeElement) element);
                }
            }
        }

        for (Element rootElement : roundEnv.getRootElements()) {
            if (rootElement instanceof TypeElement && typeUtils.isAssignable(typeUtils.erasure(rootElement.asType()), serviceType)) {
                //没有加注解的服务类
                classElements.add((TypeElement) rootElement);
            }
        }

        for (TypeElement classElement : classElements) {
            if (checkServiceClass(classElement)) {
                processServiceClass(classElement);
            }
        }

        return true;
    }

    private boolean checkServiceClass(TypeElement classElement) {
        boolean success = true;
        boolean isService = typeUtils.isAssignable(typeUtils.erasure(classElement.asType()), serviceType);

        if (isService && classElement.getNestingKind().isNested()) {
            printError("service class cannot is nested", classElement);
        }

        if (!isService && classElement.getAnnotation(ProxyConstructors.class) != null) {
            printError("non service class cannot use annotation " + ProxyConstructors.class.getSimpleName(), classElement);
        }

        for (ExecutableElement methodElement : getMethodElements(classElement, false)) {
            if (illegalMethodNamePattern.matcher(methodElement.getSimpleName()).matches()) {
                success = false;
                printError("the name of the method is illegal", methodElement);
            }

            if (methodElement.getAnnotation(Endpoint.class) != null) {
                if (!isService) {
                    success = false;
                    printError("endpoint method cannot declare in non service class", methodElement);
                } else {
                    for (Modifier illegalModifier : illegalMethodModifiers) {
                        if (methodElement.getModifiers().contains(illegalModifier)) {
                            success = false;
                            printError("endpoint method cant not declare one of " + illegalMethodModifiers, methodElement);
                        }
                    }
                }
            }
        }

        return success;
    }

    private void processServiceClass(TypeElement classElement) {
        ServiceClass serviceClass = new ServiceClass(classElement.getQualifiedName().toString());
        serviceClass.setAbs(classElement.getModifiers().contains(Modifier.ABSTRACT));
        serviceClass.setComment(elementUtils.getDocComment(classElement));
        serviceClass.setCustomProxyPath(proxyPath != null);

        if (!classElement.getTypeParameters().isEmpty()) {
            serviceClass.setTypeParametersStr("<" + classElement.getTypeParameters() + ">");
            serviceClass.setTypeParameterBounds(processTypeParameterBounds(classElement.getTypeParameters()));
        }

        DeclaredType superClassType = (DeclaredType) classElement.getSuperclass();
        TypeElement superClassElement = (TypeElement) superClassType.asElement();
        serviceClass.setSuperName(superClassElement.getQualifiedName().toString());
        if (!superClassType.getTypeArguments().isEmpty()) {
            serviceClass.setSuperTypeParameters(("<" + superClassType.getTypeArguments() + ">").replace(",", ", "));
        }

        TypeMirror serviceIdType = getServiceIdType(classElement);
        serviceClass.setIdType(serviceIdType.toString());

        ProxyConstructors proxyConstructors = classElement.getAnnotation(ProxyConstructors.class);
        if (proxyConstructors != null) {
            int[] value = proxyConstructors.value();
            if (value != null) {
                serviceClass.setProxyConstructors(Arrays.stream(value).boxed().collect(Collectors.toSet()));
            }
        }

        int methodId = getStartMethodId(classElement);
        Set<ExecutableElement> methodElements = getMethodElements(classElement, true);

        for (ExecutableElement methodElement : methodElements) {
            ServiceMethod serviceMethod = processServiceMethod(methodElement);
            serviceMethod.setId(methodId++);
            serviceMethod.setServiceClass(serviceClass);
            serviceClass.getMethods().add(serviceMethod);
        }

        try {
            serviceClass.prepare();
            generateProxy(serviceClass);
            generateCaller(serviceClass);
        } catch (IOException e) {
            printError(e);
        }
    }

    /**
     * 服务ID泛型的实际类型
     */
    private TypeMirror getServiceIdType(TypeElement classElement) {
        TypeElement tempClassElement = classElement;
        List<DeclaredType> ancestorClassTypes = new ArrayList<>();

        while (true) {
            DeclaredType superClassType = (DeclaredType) tempClassElement.getSuperclass();
            ancestorClassTypes.add(superClassType);
            if (typeUtils.isSameType(typeUtils.erasure(superClassType), serviceType)) {
                break;
            }
            tempClassElement = (TypeElement) superClassType.asElement();
        }

        TypeMirror idTypeMirror = null;

        for (int i = ancestorClassTypes.size() - 1; i >= 0; i--) {
            DeclaredType ancestorType1 = ancestorClassTypes.get(i);

            if (idTypeMirror == null) {
                idTypeMirror = ancestorType1.getTypeArguments().get(0);
            }

            if (idTypeMirror.getKind() == TypeKind.TYPEVAR) {
                DeclaredType ancestorType2 = (DeclaredType) ancestorType1.asElement().asType();
                int j = 0;
                for (TypeMirror typeArgument : ancestorType2.getTypeArguments()) {
                    if (typeArgument.toString().equals(idTypeMirror.toString())) {
                        break;
                    }
                    j++;
                }

                idTypeMirror = ancestorType1.getTypeArguments().get(j);

                if (idTypeMirror.getKind() == TypeKind.DECLARED) {
                    break;
                }
            }
        }

        return idTypeMirror;
    }

    private Set<ExecutableElement> getMethodElements(TypeElement classElement, boolean onlyEndpoint) {
        Set<ExecutableElement> methodElements = new LinkedHashSet<>();

        for (Element memberElement : classElement.getEnclosedElements()) {
            if (memberElement.getKind() == ElementKind.METHOD && (!onlyEndpoint || memberElement.getAnnotation(Endpoint.class) != null)) {
                methodElements.add((ExecutableElement) memberElement);
            }
        }

        return methodElements;
    }

    private int getStartMethodId(TypeElement classElement) {
        DeclaredType superClassType = (DeclaredType) classElement.getSuperclass();
        TypeElement superClassElement = (TypeElement) superClassType.asElement();

        if (typeUtils.isSameType(typeUtils.erasure(superClassType), serviceType)) {
            return 1;
        }

        int superStartMethodId = getStartMethodId(superClassElement);
        Set<ExecutableElement> superMethodElements = getMethodElements(superClassElement, true);

        return superStartMethodId + superMethodElements.size();
    }

    private LinkedHashMap<String, List<String>> processTypeParameterBounds(List<? extends TypeParameterElement> typeParameterElements) {
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

        if (!executableElement.getTypeParameters().isEmpty()) {
            serviceMethod.setTypeParametersStr("<" + executableElement.getTypeParameters() + ">");
            serviceMethod.setTypeParameterBounds(processTypeParameterBounds(executableElement.getTypeParameters()));
        }

        Endpoint endpoint = executableElement.getAnnotation(Endpoint.class);
        boolean safeArgs = true;

        List<? extends VariableElement> parameters = executableElement.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            VariableElement parameter = parameters.get(i);
            TypeMirror parameterType = parameter.asType();
            String parameterTypeStr = parameterType.toString();

            if (executableElement.isVarArgs() && i == parameters.size() - 1) {
                parameterTypeStr = parameterTypeStr.replace("[]", "...");
            }

            serviceMethod.addParameter(parameter.getSimpleName(), parameterTypeStr);
            if (!ConstantUtils.isConstantType(parameterType)) {
                safeArgs = false;
            }
        }

        TypeMirror returnType = executableElement.getReturnType();

        if (returnType.getKind().isPrimitive()) {
            serviceMethod.setReturnType(typeUtils.boxedClass((PrimitiveType) returnType).asType().toString());
        } else if (returnType.getKind() == TypeKind.VOID) {
            serviceMethod.setReturnType(Void.class.getSimpleName());
        } else if (typeUtils.isAssignable(typeUtils.erasure(returnType), promiseType)) {
            serviceMethod.setReturnType(((DeclaredType) returnType).getTypeArguments().get(0).toString());
        } else {
            serviceMethod.setReturnType(returnType.toString());
        }

        if (!safeArgs) {
            safeArgs = endpoint.safeArgs();
        }

        boolean safeReturn = ConstantUtils.isConstantType(returnType);
        if (!safeReturn) {
            safeReturn = endpoint.safeReturn();
        }

        serviceMethod.setSafeArgs(safeArgs);
        serviceMethod.setSafeReturn(safeReturn);

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
        JavaFileObject callerFile = filer.createSourceFile(serviceClass.getFullName() + "Caller");

        try (Writer callerWriter = callerFile.openWriter()) {
            callerTemplate.process(serviceClass, callerWriter);
        } catch (Exception e) {
            printError(e);
        }
    }

}
