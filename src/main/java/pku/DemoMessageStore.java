package pku;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {
	static final DemoMessageStore store = new DemoMessageStore();

	//给每个consumer对应一个流
	HashMap<String, BufferedInputStream> inMap = new HashMap<String, BufferedInputStream>();


//	File file=new File("/Users/hejiale/Documents/codes/JAVA/JavaMQ/data");//local test
//	File file=new File("data/data");//online test

	String pathName="/Users/hejiale/Documents/codes/JAVA/JavaMQ/data/";
//	String pathName="data/";

	String splitStr=",";

	BufferedOutputStream bufferedOutputStream;
	BufferedInputStream bufferedInputStream;

	// 加锁保证线程安全
	/**
	 * @param msg
	 */
	public synchronized void push(ByteMessage msg) {
		if (msg == null) {
			return;
		}

		try {

			String topic=msg.headers().getString("Topic");
			FileOutputStream fileOutputStream=new FileOutputStream(pathName+topic,true);
			bufferedOutputStream=new BufferedOutputStream(fileOutputStream);

			//写data
			bufferedOutputStream.write(getBytes(msg.getBody().length));
			bufferedOutputStream.write(msg.getBody());

			//写头部
			String totalHeader="";
			totalHeader=totalHeader
					+msg.headers().getObj(MessageHeader.MESSAGE_ID).toString()+splitStr
					+msg.headers().getObj(MessageHeader.TOPIC).toString()+splitStr
					+msg.headers().getObj(MessageHeader.BORN_TIMESTAMP).toString()+splitStr
					+msg.headers().getObj(MessageHeader.BORN_HOST).toString()+splitStr
					+msg.headers().getObj(MessageHeader.STORE_TIMESTAMP).toString()+splitStr
					+msg.headers().getObj(MessageHeader.STORE_HOST).toString()+splitStr
					+msg.headers().getObj(MessageHeader.START_TIME).toString()+splitStr
					+msg.headers().getObj(MessageHeader.STOP_TIME).toString()+splitStr
					+msg.headers().getObj(MessageHeader.TIMEOUT).toString()+splitStr
					+msg.headers().getObj(MessageHeader.PRIORITY).toString()+splitStr
					+msg.headers().getObj(MessageHeader.RELIABILITY).toString()+splitStr
					+msg.headers().getObj(MessageHeader.SEARCH_KEY).toString()+splitStr
					+msg.headers().getObj(MessageHeader.SCHEDULE_EXPRESSION).toString()+splitStr
					+msg.headers().getObj(MessageHeader.SHARDING_KEY).toString()+splitStr
					+msg.headers().getObj(MessageHeader.SHARDING_PARTITION).toString()+splitStr
					+msg.headers().getObj(MessageHeader.TRACE_ID).toString()+splitStr
			;
			bufferedOutputStream.write(getBytes(totalHeader.length()));
			bufferedOutputStream.write(totalHeader.getBytes());

			//写到磁盘
			bufferedOutputStream.flush();
			bufferedOutputStream.close();

		}catch (Exception e){
			e.printStackTrace();
		}

	}

	// 加锁保证线程安全
	public synchronized ByteMessage pull(String topic) {

		try {
			String currentThreadTopic=Thread.currentThread().getName()+topic;
			if (!inMap.containsKey(currentThreadTopic)){
				inMap.put(currentThreadTopic,new BufferedInputStream(new FileInputStream(pathName+topic)));
			}
			bufferedInputStream=inMap.get(currentThreadTopic);
			if (bufferedInputStream.available() ==0) {
				return null;
			}

			DefaultMessage msg;

			//读data
			byte[] lenByte = new byte[4];
			bufferedInputStream.read(lenByte);
			int len=getInteger(lenByte);
			byte[] body=new byte[len];
			bufferedInputStream.read(body);
			msg=new DefaultMessage(body);

			//读header
			byte[] headerNum=new byte[4];
			bufferedInputStream.read(headerNum);
			int headerLength=getInteger(headerNum);
			byte[] header=new byte[headerLength];
			bufferedInputStream.read(header);
			//给msg添加头部
			putHeaderFromBytes(msg,header);

			return msg;
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	//根据headerName判断其value是什么类型，int返回1，long返回2，double返回3，string返回4
	private byte headerValueTypeJudge(String headerName){
		if (headerName.equals("MessageId")) return 1;
		if (headerName.equals("Topic")) return 4;
		if (headerName.equals("BornTimestamp")) return 2;
		if (headerName.equals("BornHost")) return 4;
		if (headerName.equals("StoreTimestamp")) return 2;
		if (headerName.equals("StoreHost")) return 4;
		if (headerName.equals("StartTime")) return 2;
		if (headerName.equals("StopTime")) return 2;
		if (headerName.equals("Timeout")) return 1;
		if (headerName.equals("Priority")) return 1;
		if (headerName.equals("Reliability")) return 1;
		if (headerName.equals("SearchKey")) return 4;
		if (headerName.equals("ScheduleExpression")) return 4;
		if (headerName.equals("ShardingKey")) return 3;
		if (headerName.equals("ShardingPartition")) return 3;
		if (headerName.equals("TraceId")) return 4;
		return 0;
	}

	//int到4位byte
	private static byte[] getBytes(int bodyLength){
		byte[] byteBodyLength = new byte[4];
		for (int i = 3; i >= 0; i--){
			byteBodyLength[i] = (byte) (bodyLength & 0x000000ff);
			bodyLength >>= 8;
		}
		return byteBodyLength;
	}

	//4位byte到int
	private static int getInteger(byte[] lenByte){
		int lenInteger=(lenByte[3] & 0x000000ff)|(lenByte[2]<<8 & 0x000000ff)
				|(lenByte[1]<<16 & 0x000000ff)|(lenByte[0]<<24 & 0x000000ff);
		return lenInteger;
	}

	//给msg放入header信息
	private void putHeaderFromBytes(DefaultMessage msg,byte[] header){

		String headerStr=new String(header);
		String[] headers=headerStr.split(splitStr);
		msg.putHeaders(MessageHeader.MESSAGE_ID, Integer.parseInt(headers[0]));
		msg.putHeaders(MessageHeader.TOPIC, headers[1]);
		msg.putHeaders(MessageHeader.BORN_TIMESTAMP, Long.parseLong(headers[2]));
		msg.putHeaders(MessageHeader.BORN_HOST, headers[3]);
		msg.putHeaders(MessageHeader.STORE_TIMESTAMP, Long.parseLong(headers[4]));
		msg.putHeaders(MessageHeader.STORE_HOST, headers[5]);
		msg.putHeaders(MessageHeader.START_TIME, Long.parseLong(headers[6]));
		msg.putHeaders(MessageHeader.STOP_TIME, Long.parseLong(headers[7]));
		msg.putHeaders(MessageHeader.TIMEOUT, Integer.parseInt(headers[8]));
		msg.putHeaders(MessageHeader.PRIORITY, Integer.parseInt(headers[9]));
		msg.putHeaders(MessageHeader.RELIABILITY, Integer.parseInt(headers[10]));
		msg.putHeaders(MessageHeader.SEARCH_KEY, headers[11]);
		msg.putHeaders(MessageHeader.SCHEDULE_EXPRESSION, headers[12]);
		msg.putHeaders(MessageHeader.SHARDING_KEY, Double.parseDouble(headers[13]));
		msg.putHeaders(MessageHeader.SHARDING_PARTITION, Double.parseDouble(headers[14]));
		msg.putHeaders(MessageHeader.TRACE_ID, headers[15]);

	}


}
