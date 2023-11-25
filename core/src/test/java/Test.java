// Java program to demonstrate 
// getConstructor() method 

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import com.trilobita.engine.server.functionable.examples.aggregators.EdgeSumAggregator;
import com.trilobita.engine.server.functionable.examples.combiners.MaxCombiner;

public class Test {

	public Test() {
	}

	public static void main(String[] args)
			throws ClassNotFoundException, NoSuchMethodException {
		Functionable<?> functionable = initFunctionableInstance(EdgeSumAggregator.class.getName(), "EDGE_SUM_AGG");
		Functionable<?> functionable1 = initFunctionableInstance(MaxCombiner.class.getName(), null);
		return functionable;

	}

	private static Functionable<?> initFunctionableInstance(String className, String topic)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		Class<?> cls = Class.forName(className);
		Constructor<?> constructor = cls.getConstructor();
		Constructor<?> constructorWithname = cls.getConstructor(String.class);
		Functionable<?> functionable = topic == null
				? (Functionable<?>) constructor.newInstance()
				: (Functionable<?>) constructorWithname.newInstance(topic);
		return functionable;
	}
}
