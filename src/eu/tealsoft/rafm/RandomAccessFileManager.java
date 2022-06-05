package eu.tealsoft.rafm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Wrapper around a RandomAccessFile to write and read byte array records. File
 * format: |int: deleted 0/1|int: record key|int: record bytes length|byte[]:
 * bytes|
 * 
 * @author id854906
 *
 */
public class RandomAccessFileManager implements Closeable, AutoCloseable {
	public static final String ACCESS_R = "r";
	public static final String ACCESS_RW = "rw";

	private static final long BYTES_DELETED = 0l;
	private static final long BYTES_KEY = 4l;
	private static final long BYTES_LENGTH = 8l;
	private static final long BYTES_HEADER = 12l;

	private static final int DELETED_TRUE = 1;
	private static final int DELETED_FALSE = 0;

	private static final int RECORD_POS = 0;
	private static final int RECORD_KEY = 1;
	private static final int RECORD_LENGTH = 2;
	private static final int RECORD = 3;

	private File randomAccessFile;
	private RandomAccessFile rafR;
	private RandomAccessFile rafRw;

	private List<Integer[]> records;
	private Map<Integer, Integer> keys;

	private int recordMinKey = 0;
	private int recordMaxKey = 0;
	private int nullKeyValue = 0;

	public RandomAccessFileManager(File randomAccessFile, int nullKeyValue) {
		this.randomAccessFile = randomAccessFile;
		this.nullKeyValue = nullKeyValue;

		recordMinKey = nullKeyValue;
		recordMaxKey = nullKeyValue;

		records = new ArrayList<>();
		keys = new HashMap<>();
	}

	private void addRecord(int pos, int key, int length) {
		if (recordMinKey == nullKeyValue) {
			recordMinKey = key;
		} else {
			recordMinKey = Math.min(recordMinKey, key);
		}

		if (recordMaxKey == nullKeyValue) {
			recordMaxKey = key;
		} else {
			recordMaxKey = Math.max(recordMaxKey, key);
		}

		Integer[] record = new Integer[RECORD];
		record[RECORD_POS] = pos;
		record[RECORD_KEY] = key;
		record[RECORD_LENGTH] = length;

		records.add(record);
		keys.put(key, records.size() - 1);
	}

	@Override
	public void close() throws IOException {
		records.clear();
		keys.clear();

		if (rafR != null) {
			rafR.close();
		}

		if (rafRw != null) {
			rafRw.close();
		}
	}

	private byte[] compress(byte[] data) throws IOException {
		Deflater deflater = new Deflater();
		deflater.setInput(data);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
		deflater.finish();

		byte[] buffer = new byte[1024];
		while (!deflater.finished()) {
			int count = deflater.deflate(buffer);
			outputStream.write(buffer, 0, count);
		}
		outputStream.close();

		return outputStream.toByteArray();
	}

	/**
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public boolean containsRecordKey(int key) throws IOException {
		return getKeys().containsKey(key);
	}

	private void deleteRecord(int position, Integer[] rec) throws IOException {
		RandomAccessFile raf = getRandomAccessFileRw();
		raf.seek(rec[RECORD_POS]);
		raf.writeInt(DELETED_TRUE);

		records.remove(position);
		keys.remove(rec[RECORD_KEY]);
	}

	/**
	 * @param key
	 * @throws IOException
	 */
	public void deleteRecordByKey(int key) throws IOException {
		if (!containsRecordKey(key)) {
			throw new IOException("Record key[" + key + "] not found.");

		} else {
			int position = getKeys().get(key);
			Integer[] rec = getRecords().get(position);
			deleteRecord(position, rec);
		}
	}

	/**
	 * @param position
	 * @throws IOException
	 */
	public void deleteRecordByPosition(int position) throws IOException {
		if (position < 0) {
			throw new IOException("Position [" + position + "] is not valid.");

		} else if (position >= getRecordCount()) {
			throw new IOException("Position [" + position + "] is bigger than the position of the last record ["
					+ (getRecordCount() - 1) + "].");

		} else {
			Integer[] rec = getRecords().get(position);
			deleteRecord(position, rec);
		}
	}

	private byte[] extract(byte[] data) throws IOException, DataFormatException {
		Inflater inflater = new Inflater();
		inflater.setInput(data);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

		byte[] buffer = new byte[1024];
		while (!inflater.finished()) {
			int count = inflater.inflate(buffer);
			outputStream.write(buffer, 0, count);
		}
		outputStream.close();

		return outputStream.toByteArray();
	}

	private Map<Integer, Integer> getKeys() throws IOException {
		loadRecords();
		return keys;
	}

	private Object getObject(byte[] bytes) throws IOException {
		Object result = null;

		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
				ObjectInput input = new ObjectInputStream(bis)) {
			result = input.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("Object class not found.", e);
		}

		return result;
	}

	private RandomAccessFile getRandomAccessFileR() throws FileNotFoundException {
		if (rafR == null) {
			rafR = new RandomAccessFile(randomAccessFile, ACCESS_R);
		}
		return rafR;
	}

	private RandomAccessFile getRandomAccessFileRw() throws FileNotFoundException {
		if (rafRw == null) {
			rafRw = new RandomAccessFile(randomAccessFile, ACCESS_RW);
		}
		return rafRw;
	}

	/**
	 * @return
	 * @throws IOException
	 */
	public int getRecordCount() throws IOException {
		return getRecords().size();
	}

	/**
	 * @return
	 * @throws IOException
	 */
	public int getRecordMaxKey() throws IOException {
		loadRecords();
		return recordMaxKey;
	}

	/**
	 * @return
	 * @throws IOException
	 */
	public int getRecordMinKey() throws IOException {
		loadRecords();
		return recordMinKey;
	}

	private List<Integer[]> getRecords() throws IOException {
		loadRecords();
		return records;
	}

	private void loadRecords() throws IOException {
		if (records.isEmpty()) {
			keys.clear();
			recordMinKey = nullKeyValue;
			recordMaxKey = nullKeyValue;

			if (randomAccessFile.canRead()) {
				RandomAccessFile raf = getRandomAccessFileR();
				long rafLength = raf.length();

				int pos = 0;
				int deleted = 0;
				int recKey = 0;
				int recLength = 0;
				while (pos < rafLength) {
					raf.seek(pos + BYTES_DELETED);
					deleted = raf.readInt();

					raf.seek(pos + BYTES_KEY);
					recKey = raf.readInt();

					raf.seek(pos + BYTES_LENGTH);
					recLength = raf.readInt();

					if (deleted == DELETED_FALSE) {
						addRecord(pos, recKey, recLength);
					}
					pos += BYTES_HEADER + recLength;
				}
			}
		}
	}

	public void purge() throws IOException {
		if (randomAccessFile.canRead()) {
			RandomAccessFile raf = getRandomAccessFileR();
			long rafLength = raf.length();

			int pos = 0;
			int deleted = 0;
			int recKey = 0;
			int recLength = 0;
			while (pos < rafLength) {
				raf.seek(pos + BYTES_DELETED);
				deleted = raf.readInt();

				raf.seek(pos + BYTES_KEY);
				recKey = raf.readInt();

				raf.seek(pos + BYTES_LENGTH);
				recLength = raf.readInt();

				if (deleted == DELETED_FALSE) {
					addRecord(pos, recKey, recLength);
				}
				pos += BYTES_HEADER + recLength;
			}
		}
	}

	/**
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public <T> T readObjectByKey(Class<T> type, int key) throws IOException {
		return type.cast(getObject(readRecordByKey(key)));
	}

	/**
	 * @param position
	 * @return
	 * @throws IOException
	 */
	public <T> T readObjectByPosition(Class<T> type, int position) throws IOException {
		return type.cast(getObject(readRecordByPosition(position)));
	}

	/**
	 * @param key
	 * @return
	 * @throws IOException
	 * @throws DataFormatException
	 */
	public byte[] readRecordByKey(int key) throws IOException {
		if (!containsRecordKey(key)) {
			throw new IOException("Record key[" + key + "] not found.");

		} else {
			Integer[] rec = getRecords().get(getKeys().get(key));
			try {
				RandomAccessFile raf = getRandomAccessFileR();
				raf.seek(rec[RECORD_POS] + BYTES_HEADER);
				byte[] zip = new byte[rec[RECORD_LENGTH]];
				raf.readFully(zip);
				return extract(zip);
			} catch (DataFormatException e) {
				throw new IOException("Can not read record key[" + key + "].", e);
			}
		}
	}

	/**
	 * @param position
	 * @return
	 * @throws IOException
	 * @throws DataFormatException
	 */
	public byte[] readRecordByPosition(int position) throws IOException {
		if (position < 0) {
			throw new IOException("Position [" + position + "] is not valid.");

		} else if (position >= getRecordCount()) {
			throw new IOException("Position [" + position + "] is bigger than the position of the last record ["
					+ (getRecordCount() - 1) + "].");

		} else {
			try {
				RandomAccessFile raf = getRandomAccessFileR();
				Integer[] rec = getRecords().get(position);
				raf.seek(rec[RECORD_POS] + BYTES_HEADER);
				byte[] zip = new byte[rec[RECORD_LENGTH]];
				raf.readFully(zip);
				return extract(zip);
			} catch (DataFormatException e) {
				throw new IOException("Can not read record with position [" + position + "].", e);
			}
		}
	}

	/**
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public String readStringByKey(int key) throws IOException {
		return new String(readRecordByKey(key), StandardCharsets.UTF_8);
	}

	/**
	 * @param position
	 * @return
	 * @throws IOException
	 */
	public String readStringByPosition(int position) throws IOException {
		return new String(readRecordByPosition(position), StandardCharsets.UTF_8);
	}

	/**
	 * @param key
	 * @param object
	 * @throws IOException
	 */
	public void writeObject(int key, Serializable object) throws IOException {
		if (object == null) {
			writeRecord(key, new byte[0]);

		} else {
			try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
					ObjectOutput output = new ObjectOutputStream(bytes)) {
				output.writeObject(object);
				output.flush();
				writeRecord(key, bytes.toByteArray());
			}
		}
	}

	/**
	 * @param object
	 * @throws IOException
	 */
	public void writeObject(Serializable object) throws IOException {
		writeObject(nullKeyValue, object);
	}

	/**
	 * @param object
	 * @throws IOException
	 */
	public void writeRecord(byte[] record) throws IOException {
		writeRecord(nullKeyValue, record);
	}

	/**
	 * @param key
	 * @param record
	 * @throws IOException
	 */
	public void writeRecord(int key, byte[] record) throws IOException {
		if (key != nullKeyValue && containsRecordKey(key)) {
			throw new IOException("Duplicate record key[" + key + "].");

		} else {
			RandomAccessFile raf = getRandomAccessFileRw();

			long rafLength = raf.length();
			raf.seek(rafLength + BYTES_DELETED);
			raf.writeInt(DELETED_FALSE);

			raf.seek(rafLength + BYTES_KEY);
			raf.writeInt(key);

			raf.seek(rafLength + BYTES_LENGTH);
			if (record == null || record.length == 0) {
				raf.writeInt(0);
				addRecord((int) rafLength, key, 0);

			} else {
				byte[] zip = compress(record);
				raf.writeInt(zip.length);
				raf.seek(rafLength + BYTES_HEADER);
				raf.write(zip);
				addRecord((int) rafLength, key, zip.length);
			}
		}
	}

	/**
	 * @param key
	 * @param string
	 * @throws IOException
	 */
	public void writeString(int key, String string) throws IOException {
		writeRecord(key, (string == null) ? new byte[0] : string.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * @param object
	 * @throws IOException
	 */
	public void writeString(String string) throws IOException {
		writeString(nullKeyValue, string);
	}

}