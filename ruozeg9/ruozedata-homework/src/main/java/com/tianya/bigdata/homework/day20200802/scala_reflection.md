# 作业 翻译https://docs.scala-lang.org/overviews/reflection/overview.html

```$xslt

Reflection is the ability of a program to inspect, and possibly even modify itself. It has a long history across object-oriented, functional, and logic programming paradigms.
 While some languages are built around reflection as a guiding principle, many languages progressively evolve their reflection abilities over time.
反射是程序检查甚至修改自身的能力。它在面向对象，功能和逻辑编程范例方面有着悠久的历史。尽管一些语言是围绕反射作为指导原则而构建的，但是许多语言会随着时间的推移逐步发展其反射能力。
```


```$xslt

Reflection involves the ability to reify (ie. make explicit) otherwise-implicit elements of a program. 
These elements can be either static program elements like classes, methods, or expressions, or dynamic elements like the current continuation or execution events such as method invocations and field accesses. 
One usually distinguishes between compile-time and runtime reflection depending on when the reflection process is performed. 
Compile-time reflection is a powerful way to develop program transformers and generators, 
while runtime reflection is typically used to adapt the language semantics or to support very late binding between software components.
反射涉及对程序中的其他隐含元素进行修饰（即，使之明确）的能力。这些元素可以是静态程序元素，
例如类，方法或表达式，也可以是动态元素，例如当前的延续或执行事件，例如方法调用和字段访问。
通常根据执行反射过程的时间来区分编译时和运行时反射。
编译时反射是开发程序转换器和生成器的有效方法，而运行时反射通常用于调整语言语义或支持软件组件之间的后期绑定。

```


```

Until 2.10, Scala has not had any reflection capabilities of its own.
 Instead, one could use part of the Java reflection API, namely that dealing with providing the ability to dynamically inspect classes and objects and access their members.
 However, many Scala-specific elements are unrecoverable under standalone Java reflection, which only exposes Java elements (no functions, no traits) 
and types (no existential, higher-kinded, path-dependent and abstract types).
 In addition, Java reflection is also unable to recover runtime type info of Java types that are generic at compile-time; 
a restriction that carried through to runtime reflection on generic types in Scala.

scala没有属于自己的反射能力，直到 2.10版本。在动态检查类和对象，以及访问对象的属性这方面，可以使用java的反射API。
然而，许多scala独有的元素在java的反射API中无法通过反射恢复出来，java的反射仅仅是暴露java相关的元素和类型（没有function和trait，也没有存在的，种类较多的，依赖路径的抽象类型）
此外，java的反射也无法得到运行时的java类型信息，通常这是在编译期用泛型来做的，这种限制导致无法在运行期拿到scala的泛型信息
```

```$xslt

In Scala 2.10, 
a new reflection library was introduced not only to address the shortcomings of Java’s runtime reflection on Scala-specific and generic types,
 but to also add a more powerful toolkit of general reflective capabilities to Scala. 
 Along with full-featured runtime reflection for Scala types and generics,
  Scala 2.10 also ships with compile-time reflection capabilities, in the form of macros, 
  as well as the ability to reify Scala expressions into abstract syntax trees.

在Scala 2.10中，引入了一个新的反射库，不仅解决了Java在特定于Scala的类型和泛型类型上的运行时反射的缺点，
而且还向Scala添加了更强大的常规反射功能的工具包。
除了针对Scala类型和泛型的全功能运行时反射外，Scala 2.10还具有以宏形式出现的编译时反射功能，以及将Scala表达式化为抽象语法树的功能。

```

# Runtime Reflection 运行时反射

```$xslt
What is runtime reflection? Given a type or instance of some object at runtime, reflection is the ability to:

inspect the type of that object, including generic types,
to instantiate new objects,
or to access or invoke members of that object.
Let’s jump in and see how to do each of the above with a few examples.

在运行时给定一个类型或者对象，我们通过反射可以：
1.检查对象的类型和泛型
2.实例化一个新的对象
3.访问对象的属性或者执行对象方法
下面给几个例子
```
## Examples 例子
### INSPECTING A RUNTIME TYPE (INCLUDING GENERIC TYPES AT RUNTIME)
```$xslt
As with other JVM languages, Scala’s types are erased at compile time. 
This means that if you were to inspect the runtime type of some instance, 
that you might not have access to all type information that the Scala compiler has available at compile time.
与其他JVM语言一样，Scala的类型在编译时会被擦除。这意味着如果我们想在运行期检查一些实例，则可能无法拿到Scala编译器在编译时可用的所有类型信息。

TypeTags can be thought of as objects which carry along all type information available at compile time, to runtime. 
Though, it’s important to note that TypeTags are always generated by the compiler. 
This generation is triggered whenever an implicit parameter or context bound requiring a TypeTag is used.
 This means that, typically, one can only obtain a TypeTag using implicit parameters or context bounds.
TypeTags 可以把它看做是运行期携带了所有编译期信息的对象。不过，TypeTag始终由编译器生成，这点很重要。每当使用需要TypeTag的隐式参数或上下文绑定时，都会触发TypeTags的生成。
这意味着通常情况下，只能使用隐式参数或上下文范围来获取TypeTag。

```
```$xslt
scala> import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.{universe=>ru}

scala> val l = List(1,2,3)
l: List[Int] = List(1, 2, 3)

scala> def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
getTypeTag: [T](obj: T)(implicit evidence$1: ru.TypeTag[T])ru.TypeTag[T]

scala> val theType = getTypeTag(l).tpe
theType: ru.Type = List[Int]
```

```$xslt
In the above, we first import scala.reflect.runtime.
universe (it must always be imported in order to use TypeTags), and we create a List[Int] called l. 
Then, we define a method getTypeTag which has a type parameter T that has a context bound
 (as the REPL shows, this is equivalent to defining an implicit “evidence” parameter, which causes the compiler to generate a TypeTag for T).
 Finally, we invoke our method with l as its parameter, and call tpe which returns the type contained in the TypeTag. 
As we can see, we get the correct, complete type (including List’s concrete type argument), List[Int].

Once we have obtained the desired Type instance, we can inspect it, e.g.:
scala> val decls = theType.decls.take(10)
decls: Iterable[ru.Symbol] = List(constructor List, method companion, method isEmpty, method head, method tail, method ::, method :::, method reverse_:::, method mapConserve, method ++)
1.首先要导包
2.然后我们定义了一个List[Int],变量l来接收
3.接着我们定义了一个方法getTypeTag，这个方法的参数是有上下文绑定的
4.最后我们调用这个getTypeTag方法，以l作为参数，并调用tye方法来返回TypeTag里面包含的类型信息
这样我们就能够获取到完整类型了
一旦获得所需的Type实例，就可以对其进行检查
```

## INSTANTIATING A TYPE AT RUNTIME

```$xslt
Types obtained through reflection can be instantiated by invoking their constructor using an appropriate “invoker” mirror (mirrors are expanded upon below).
 Let’s walk through an example using the REPL
scala> case class Person(name: String)
defined class Person

scala> val m = ru.runtimeMirror(getClass.getClassLoader)
m: scala.reflect.runtime.universe.Mirror = JavaMirror with ...

通过反射获得的类型可以通过使用适当的“invoker”镜像调用构造器来实例化（镜像在下面进行了扩展）。让我们来看一个使用REPL的示例
In the first step we obtain a mirror m which makes all classes and types available that are loaded by the current classloader, including class Person
1.第一步我们先获取到一个镜像，镜像里包含了当前类加载器加载的所有类和类型的信息，包括了Person的信息
scala> val classPerson = ru.typeOf[Person].typeSymbol.asClass
classPerson: scala.reflect.runtime.universe.ClassSymbol = class Person

scala> val cm = m.reflectClass(classPerson)
cm: scala.reflect.runtime.universe.ClassMirror = class mirror for Person (bound to null)

The second step involves obtaining a ClassMirror for class Person using the reflectClass method. The ClassMirror provides access to the constructor of class Person
scala> val ctor = ru.typeOf[Person].decl(ru.termNames.CONSTRUCTOR).asMethod
ctor: scala.reflect.runtime.universe.MethodSymbol = constructor Person
2.第二步使用reflectClass方法为类Person获取ClassMirror。 ClassMirror提供对Person类的构造函数的访问

The symbol for Persons constructor can be obtained using only the runtime universe ru by looking it up in the declarations of type Person.
scala> val ctorm = cm.reflectConstructor(ctor)
ctorm: scala.reflect.runtime.universe.MethodMirror = constructor mirror for Person.<init>(name: String): Person (bound to null)

scala> val p = ctorm("Mike")
p: Any = Person(Mike)
通过查看Person类的声明 仅适用运行时的universe ru就可以获得Person的构造器符号

```

# ACCESSING AND INVOKING MEMBERS OF RUNTIME TYPES

```$xslt
In general, members of runtime types are accessed using an appropriate “invoker” mirror (mirrors are expanded upon below). Let’s walk through an example using the REPL:
scala> case class Purchase(name: String, orderNumber: Int, var shipped: Boolean)
defined class Purchase

scala> val p = Purchase("Jeff Lebowski", 23819, false)
p: Purchase = Purchase(Jeff Lebowski,23819,false)

通常，我们可以通过使用适当的调用程序镜像来访问运行时的成员的类型

In this example, we will attempt to get and set the shipped field of Purchase p, reflectively
scala> import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.{universe=>ru}

scala> val m = ru.runtimeMirror(p.getClass.getClassLoader)
m: scala.reflect.runtime.universe.Mirror = JavaMirror with ...

在这个案例中，我们试图去获取和设置Purchase p 的字段
As we did in the previous example, we’ll begin by obtaining a mirror m, 
which makes all classes and types available that are loaded by the classloader that also loaded the class of p (Purchase), 
which we need in order to access member shipped
```

