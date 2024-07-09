package quan.rpc;

import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SupportedOptions({"rpcProxyPath", "rpcProxyLinkToService"})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes({"quan.rpc.Endpoint", "quan.rpc.ProxyConstructors"})
public class Generator extends AbstractProcessor {

    private Types types;

    private Elements elements;

    private TypeMirror serviceType;

    private TypeMirror promiseType;

    private SourceLineResolver sourceLineResolver;

    /**
     * 自定义代理类的生成路径
     */
    private String proxyPath;

    /**
     * 代理类和方法是否链接到服务类和方法
     */
    private String proxyLinkToService;

    /**
     * 非法的服务方法名格式
     */
    private final Pattern illegalMethodNamePattern = Pattern.compile(".*\\$");

    /**
     * 非法的服务方法修饰符
     */
    private final List<Modifier> illegalMethodModifiers = Arrays.asList(Modifier.PRIVATE, Modifier.STATIC, Modifier.ABSTRACT);

    private final List<Class<? extends Annotation>> serviceAnnotations = Arrays.asList(Endpoint.class, ProxyConstructors.class);

    private Template proxyTemplate;

    private Template invokerTemplate;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        types = processingEnv.getTypeUtils();
        elements = processingEnv.getElementUtils();
        serviceType = types.erasure(elements.getTypeElement(Service.class.getName()).asType());
        promiseType = types.erasure(elements.getTypeElement(Promise.class.getName()).asType());
        proxyPath = processingEnv.getOptions().get("rpcProxyPath");
        proxyLinkToService = processingEnv.getOptions().get("rpcProxyLinkToService");
        sourceLineResolver = SourceLineResolver.newInstance();

        try {
            Configuration freemarkerCfg = new Configuration(Configuration.VERSION_2_3_23);
            freemarkerCfg.setClassForTemplateLoading(getClass(), "");
            freemarkerCfg.setDefaultEncoding("UTF-8");
            proxyTemplate = freemarkerCfg.getTemplate("proxy.ftl");
            invokerTemplate = freemarkerCfg.getTemplate("invoker.ftl");
        } catch (IOException e) {
            error(e);
        }
    }

    private void warn(String msg, Element element) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, msg, element);
    }

    private void error(Exception e) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.toString());
        e.printStackTrace();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Set<TypeElement> serviceClassElements = new HashSet<>();
        Set<TypeElement> nonServiceClassElements = new HashSet<>();

        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                //这里会有重复
                TypeElement classElement;

                if (element instanceof TypeElement) {
                    classElement = (TypeElement) element;
                } else {
                    classElement = (TypeElement) element.getEnclosingElement();
                }

                if (types.isAssignable(types.erasure(classElement.asType()), serviceType)) {
                    serviceClassElements.add(classElement);
                } else {
                    nonServiceClassElements.add(classElement);
                }
            }
        }

        for (Element rootElement : roundEnv.getRootElements()) {
            if (types.isAssignable(types.erasure(rootElement.asType()), serviceType)) {
                serviceClassElements.add((TypeElement) rootElement);
            }
        }

        serviceClassElements.forEach(this::processServiceClass);
        nonServiceClassElements.forEach(this::processNonServiceClass);

        return true;
    }

    private void processNonServiceClass(TypeElement classElement) {
        for (Class<? extends Annotation> serviceAnnotation : serviceAnnotations) {
            if (classElement.getAnnotation(serviceAnnotation) != null) {
                warn("Annotation " + serviceAnnotation.getSimpleName() + " cannot declare in non service class", classElement);
            }
        }

        for (ExecutableElement methodElement : getMethodElements(classElement)) {
            warn("Endpoint method cannot declare in non service class", methodElement);
        }
    }


    private void processServiceClass(TypeElement classElement) {
        if (classElement.getNestingKind().isNested()) {
            warn("Service class cannot is nested", classElement);
            return;
        }

        ServiceClassDefinition serviceClassDefinition = new ServiceClassDefinition(classElement.getQualifiedName().toString());
        serviceClassDefinition.setAbstract(classElement.getModifiers().contains(Modifier.ABSTRACT));
        serviceClassDefinition.setComment(elements.getDocComment(classElement));

        if (StringUtils.isBlank(proxyLinkToService)) {
            serviceClassDefinition.setProxyLinkToService(StringUtils.isBlank(proxyPath));
        } else {
            serviceClassDefinition.setProxyLinkToService(Boolean.parseBoolean(proxyLinkToService));
        }

        if (!classElement.getTypeParameters().isEmpty()) {
            serviceClassDefinition.setTypeParametersStr("<" + classElement.getTypeParameters() + ">");
            serviceClassDefinition.setTypeParameterBounds(processTypeParameterBounds(classElement.getTypeParameters()));
        }

        DeclaredType superClassType = (DeclaredType) classElement.getSuperclass();
        TypeElement superClassElement = (TypeElement) superClassType.asElement();
        serviceClassDefinition.setSuperName(superClassElement.getQualifiedName().toString());
        if (!superClassType.getTypeArguments().isEmpty()) {
            serviceClassDefinition.setSuperTypeParameters(("<" + superClassType.getTypeArguments() + ">").replace(",", ", "));
        }

        TypeMirror serviceIdType = getServiceIdType(classElement);
        serviceClassDefinition.setIdType(serviceIdType.toString());

        ProxyConstructors proxyConstructors = classElement.getAnnotation(ProxyConstructors.class);
        if (proxyConstructors != null) {
            serviceClassDefinition.setProxyConstructors(Arrays.stream(proxyConstructors.value()).boxed().collect(Collectors.toSet()));
        }

        if (serviceClassDefinition.getIdType().equals(Object.class.getName()) && serviceClassDefinition.hasConstructor(ProxyConstructors.SHARDING_KEY)) {
            warn("The id type is Object and conflicts with the sharding key", classElement);
        }

        int methodId = getStartMethodId(classElement);
        Set<ExecutableElement> methodElements = getMethodElements(classElement);

        for (ExecutableElement methodElement : methodElements) {
            if (illegalMethodNamePattern.matcher(methodElement.getSimpleName()).matches()) {
                warn("The name of the method is illegal", methodElement);
            }

            if (methodElement.getModifiers().stream().anyMatch(illegalMethodModifiers::contains)) {
                warn("Endpoint method cant not declare one of " + illegalMethodModifiers, methodElement);
            }

            ServiceMethodDefinition serviceMethodDefinition = processServiceMethod(methodElement);
            serviceMethodDefinition.setId(methodId++);
            serviceMethodDefinition.setServiceClass(serviceClassDefinition);
            serviceClassDefinition.getMethods().add(serviceMethodDefinition);
        }

        try {
            serviceClassDefinition.prepare();
            generateProxy(serviceClassDefinition);
            generateInvoker(serviceClassDefinition);
        } catch (Exception e) {
            error(e);
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
            if (types.isSameType(types.erasure(superClassType), serviceType)) {
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

    private Set<ExecutableElement> getMethodElements(TypeElement classElement) {
        Set<ExecutableElement> methodElements = new LinkedHashSet<>();

        for (Element memberElement : classElement.getEnclosedElements()) {
            if (memberElement.getKind() == ElementKind.METHOD && memberElement.getAnnotation(Endpoint.class) != null) {
                methodElements.add((ExecutableElement) memberElement);
            }
        }

        return methodElements;
    }

    private int getStartMethodId(TypeElement classElement) {
        DeclaredType superClassType = (DeclaredType) classElement.getSuperclass();
        TypeElement superClassElement = (TypeElement) superClassType.asElement();

        if (types.isSameType(types.erasure(superClassType), serviceType)) {
            return 1;
        }

        int superStartMethodId = getStartMethodId(superClassElement);
        Set<ExecutableElement> superMethodElements = getMethodElements(superClassElement);

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

    private ServiceMethodDefinition processServiceMethod(ExecutableElement executableElement) {
        ServiceMethodDefinition serviceMethodDefinition = new ServiceMethodDefinition(executableElement.getSimpleName());
        serviceMethodDefinition.setComment(elements.getDocComment(executableElement));
        serviceMethodDefinition.setSourceLine(sourceLineResolver.resolveSourceLine(processingEnv, executableElement));

        if (!executableElement.getTypeParameters().isEmpty()) {
            serviceMethodDefinition.setTypeParametersStr("<" + executableElement.getTypeParameters() + ">");
            serviceMethodDefinition.setTypeParameterBounds(processTypeParameterBounds(executableElement.getTypeParameters()));
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

            serviceMethodDefinition.addParameter(parameter.getSimpleName(), parameterTypeStr);
            if (!ConstantUtils.isConstantType(parameterType)) {
                safeArgs = false;
            }
        }

        TypeMirror returnType = executableElement.getReturnType();

        if (returnType.getKind().isPrimitive()) {
            serviceMethodDefinition.setReturnType(types.boxedClass((PrimitiveType) returnType).asType().toString());
        } else if (returnType.getKind() == TypeKind.VOID) {
            serviceMethodDefinition.setReturnType(Void.class.getSimpleName());
        } else if (types.isAssignable(types.erasure(returnType), promiseType)) {
            serviceMethodDefinition.setReturnType(((DeclaredType) returnType).getTypeArguments().get(0).toString());
        } else {
            serviceMethodDefinition.setReturnType(returnType.toString());
        }

        if (!safeArgs) {
            safeArgs = endpoint.safeArgs();
        }

        boolean safeReturn = ConstantUtils.isConstantType(returnType);
        if (!safeReturn) {
            safeReturn = endpoint.safeReturn();
        }

        serviceMethodDefinition.setSafeArgs(safeArgs);
        serviceMethodDefinition.setSafeReturn(safeReturn);

        serviceMethodDefinition.setExpiredTime(endpoint.expiredTime());

        return serviceMethodDefinition;
    }

    private void generateProxy(ServiceClassDefinition serviceClassDefinition) throws IOException {
        Writer proxyWriter;

        if (StringUtils.isBlank(proxyPath)) {
            JavaFileObject proxyFile = processingEnv.getFiler().createSourceFile(serviceClassDefinition.getFullName() + "Proxy");
            proxyWriter = proxyFile.openWriter();
        } else {
            File path = new File(proxyPath.trim(), serviceClassDefinition.getPackageName().replace(".", "/"));
            //noinspection ResultOfMethodCallIgnored
            path.mkdirs();
            File file = new File(path, serviceClassDefinition.getName() + "Proxy.java");
            proxyWriter = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
        }

        try {
            proxyTemplate.process(serviceClassDefinition, proxyWriter);
        } catch (Exception e) {
            error(e);
        } finally {
            proxyWriter.close();
        }

    }

    private void generateInvoker(ServiceClassDefinition serviceClassDefinition) throws IOException {
        JavaFileObject invokerFile = processingEnv.getFiler().createSourceFile(serviceClassDefinition.getFullName() + "Invoker");

        try (Writer invokerWriter = invokerFile.openWriter()) {
            invokerTemplate.process(serviceClassDefinition, invokerWriter);
        } catch (Exception e) {
            error(e);
        }
    }

}
