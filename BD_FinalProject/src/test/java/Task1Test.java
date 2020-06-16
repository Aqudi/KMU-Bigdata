import bigdata.Task1;
import org.apache.hadoop.util.ToolRunner;

public class Task1Test {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task1(), new String[] {"src/test/resources/facebook.txt", "src/test/resources/task1.out"});
    }
}
