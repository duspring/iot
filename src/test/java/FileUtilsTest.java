import com.thoughtworks.constant.Constant;
import com.thoughtworks.util.FileUtils;
import org.junit.Test;

import java.net.URL;
import java.util.List;

/**
 * @author: spring du
 * @description: 单元测试
 * @date: 2021/1/6 17:39
 */
public class FileUtilsTest {

    @Test
    public void readFileTest() {
        List<String> inputs = FileUtils.readFile(Constant.INPUT_FILE_NAME);
        inputs.forEach(line -> System.out.println(line));
    }

    @Test
    public void readResourceFile() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(Constant.INPUT_FILE_NAME);
        String path = resource.getPath();
        System.out.println(path);
    }
}
