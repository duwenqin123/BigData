import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by duwenqin123 on 9/28/17.
 */

public class MyTextOutputFormat<K, V> extends TextOutputFormat<K,V> {
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getOutputName(context));
    }
}
