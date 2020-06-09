import org.apache.hadoop.util.ToolRunner;

public class Task1_Test {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task1_SimpleGraph(), new String[] {"src/test/resources/test.txt", "src/test/resources/test.out"});
    }
}
