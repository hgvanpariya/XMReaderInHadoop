package my.hari;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		String xmlString = value.toString();

		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		String value1 = "";
		try {

			Document doc = builder.build(in);
			Element root = doc.getRootElement();

			List<Element> studentList = root.getChildren();
			System.out.println("----------------------------");

			for (int temp = 0; temp < studentList.size(); temp++) {
				Element student = studentList.get(temp);
				System.out.println("\nCurrent Element :" + student.getName());
				Attribute attribute = student.getAttribute("rollno");
//				System.out.println("Student roll no : " + attribute.getValue());
//				System.out.println("First Name : "
//						+ student.getChild("firstname").getText());
//				System.out.println("Last Name : "
//						+ student.getChild("lastname").getText());
//				System.out.println("Nick Name : "
//						+ student.getChild("nickname").getText());
//				System.out.println("Marks : "
//						+ student.getChild("marks").getText());

				context.write(new Text(attribute.getValue()),
						new Text(student.getChild("marks").getText() + "_"
								+ student.getChild("firstname").getText()));
			}

//			context.write(new Text("Trial"),
//					new Text("Trial"));
			
			// String tag1 =
			// root.getChild("tag").getChild("tag1").getTextTrim();
			//
			// String tag2 = root.getChild("tag").getChild("tag1")
			// .getChild("tag2").getTextTrim();
			// value1 = tag1 + "," + tag2;

		} catch (JDOMException ex) {
			Logger.getLogger(MyMapper.class.getName()).log(Level.SEVERE, null,
					ex);
		} catch (IOException ex) {
			Logger.getLogger(MyMapper.class.getName()).log(Level.SEVERE, null,
					ex);
		}

	}

	// String mapperString = value1.toString();
	// String[] split = mapperString.split(" ");
	// for (String stringSplit : split) {
	// context.write(new Text(stringSplit), new IntWritable(1));
	// }

	// };
}
