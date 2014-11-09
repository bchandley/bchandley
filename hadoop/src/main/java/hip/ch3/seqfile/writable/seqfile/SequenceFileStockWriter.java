/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hip.ch3.seqfile.writable.seqfile;

/**
 *
 * @author bchandley
 */
import hip.ch3.seqfile.writable.StockPriceWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import java.io.File;
import java.io.IOException;
    import java.util.*;
import org.apache.commons.io.FileUtils;

    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.conf.*;
    import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
    import org.apache.hadoop.mapreduce.*;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileStockWriter extends Configured implements Tool {

    
      public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileStockWriter(), args);
    System.exit(res);
  }
      
      
      
  /**
   * Write the sequence file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {
     Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.MrIoOpts.values()).build();
    int result = cli.runCmd();
    
     if (result != 0) {
      return result;
    }
     
     
    File inputFile = new File(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));
    Configuration conf = super.getConf();
   SequenceFile.Writer writer =    //<co id="ch03_comment_seqfile_write1"/>
        SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(outputPath),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(StockPriceWritable.class),
            SequenceFile.Writer.compression(
                SequenceFile.CompressionType.BLOCK,
                new DefaultCodec())
        );
   try{
   Text key = new Text();
   for(String line : FileUtils.readLines(inputFile)  ){
   
   StockPriceWritable stock = StockPriceWritable.fromLine(line);
    System.out.println("Stock = " + stock);

        key.set(stock.getSymbol());

        writer.append(key, stock);        //<co id="ch03_comment_seqfile_write4"/>
        
   }
   }finally{
       writer.close();
   }
        return 0;
  }
}
