import bigdata.Task2;
import org.apache.hadoop.util.ToolRunner;

public class Task2Test {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task2(), new String[] {"src/test/resources/task1.out", "src/test/resources/task2.out", });
    }
}
