package org.ed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/***
 * �ļ����Ͽ�
 * 
 * <p>
 * �ĵ��ṹ����������:
 * <ul>
 * <li>
 * {repositoryStorageDir}/{name}Ϊ�����Ͽ�Ĵ洢Ŀ¼(repositoryStorageDir��nameΪ���캯������Ĳ���
 * {@link #FileRepository(String, String, Function)})</li>
 * <li>
 * {repositoryStorageDir}/{name}/*.snapshotΪ���Ͽ�����п����ļ�</li>
 * <li>
 * {repositoryStorageDir}/{name}/*.evtΪ���Ͽ�������¼��ļ�</li>
 * <li>
 * ���յ���������Ϊ{id}.snapshot��ͬ���¼���������Ϊ{id}.evt</li>
 * <li>ÿ��ҵ��ʵ���id��Ψһ��</li>
 * </ul>
 * </p>
 * <p>
 * �����ļ��ṹ��
 * <ul>
 * <li>{�¼��ļ�ƫ����}{ҵ��ʵ�����л�����}</li>
 * <li>ֻ��һ��ҵ��ʵ��</li>
 * </ul>
 * </p>
 * <p>
 * �¼��ļ��ṹ��
 * <ul>
 * <li>{���л����ݴ�С}{�¼���Ϣʵ�����л�����}</li>
 * <li>���ļ�����ҵ��ʵ�屣�ֵ������¼�</li>
 * </ul>
 * </p>
 * 
 * @author tao
 *
 * @param <T>
 */
public class FileRepository<T extends AggregateRoot> implements Repository<T>, Closeable {

	/***
	 * �����ļ���׺
	 */
	private final String SNAPSHOT_SUFFIX = ".snapshot";
	/***
	 * �¼��ļ���׺
	 */
	private final String EVENT_SUFFIX = ".evt";
	/***
	 * �����С�����Լ��Ļ����ϲ���1MΪ��죬����1Mû����
	 */
	private final int BUFFER_SIZE = 1024 * 1024;
	/***
	 * �����ӳ�ʱ�䣬��λ������
	 */
	private final int IDLE_DELAY = 100;
	/***
	 * Ϊ���ظ���ObjectOutputStream�����ѣ��Ժ��ҺõĽ������
	 */
	private final byte[] SERIALIZE_HEADER = new byte[] { (byte) 0xac, (byte) 0xed, 0x00, 0x05 };
	/***
	 * ҵ��ʵ��������
	 */
	private Function<String, T> creator;
	/***
	 * ���Ͽ�洢Ŀ¼
	 */
	private Path repositoryStorageDir;
	/***
	 * �ļ�channel����
	 */
	private Map<String, FileChannel> fileChannelCache = new HashMap<String, FileChannel>();
	/***
	 * ҵ��ʵ�建��
	 */
	private Map<String, AggregateRoot> aggRootCache = new HashMap<String, AggregateRoot>();
	/***
	 * �ļ�д�����߳�
	 */
	private ExecutorService fileWritesExecutorService;
	/***
	 * �¼��ļ�buffer��Ϊ�˼ӿ��ٶ�
	 */
	private Map<String, ByteBuffer> evtFileBufferCache = new HashMap<String, ByteBuffer>();
	/***
	 * �¼��ֽ���
	 */
	private ByteArrayOutputStream eventMessageByteArrayOutputStream;
	/***
	 * �¼����л���
	 */
	private ObjectOutputStream eventMessageObjectOutputStream;

	private Map<String, IdleScheduledFutureAndRunnableCacheEntry> idleTimers = new HashMap<String, IdleScheduledFutureAndRunnableCacheEntry>();
	private ScheduledExecutorService scheduledService;

	/***
	 * �ļ����Ͽ⹹�캯��
	 * 
	 * @param repositoryStorageDir
	 *          ���Ͽ�洢Ŀ¼
	 * @param name
	 *          ҵ������
	 * @param creator
	 *          ҵ��ʵ��������
	 */
	public FileRepository(String repositoryStorageDir, String name, Function<String, T> creator) {
		this.repositoryStorageDir = FileSystems.getDefault().getPath(repositoryStorageDir, name);
		this.creator = creator;

		if (!Files.exists(this.repositoryStorageDir))
			try {
				Files.createDirectories(this.repositoryStorageDir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		fileWritesExecutorService = Executors.newSingleThreadExecutor();
		scheduledService = Executors.newSingleThreadScheduledExecutor();
		try {
			eventMessageByteArrayOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
			eventMessageObjectOutputStream = new ObjectOutputStream(eventMessageByteArrayOutputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void snapshot(String id) {
		File file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX).toFile();

		if (file.exists()) {
			T es = load(id);

			OutputStream os = null;
			try {
				// begin������ǰ�Ŀ���
				if (fileChannelCache.containsKey(file.getPath())) {
					fileChannelCache.get(file.getPath()).close();
					fileChannelCache.remove(file.getPath());
				}

				file.delete();
				// end������ǰ�Ŀ���

				// begin���½�������
				file.createNewFile();
				os = Channels.newOutputStream(getFileChannel(file.getPath()));
				file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX).toFile();
				long positing = file.length();
				DataOutputStream dos = new DataOutputStream(os);
				dos.writeLong(positing);
				ObjectOutputStream oos = new ObjectOutputStream(dos);
				oos.writeObject(es);
				// end���ֽ�������

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T load(String id) {
		// Ϊ�˼���ҵ��ʵ��
		if (aggRootCache.containsKey(id))
			return (T) aggRootCache.get(id);

		T es = null;

		Path es_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);

		// ���id�Ǵ�getIds������ȡ����Ӧ�ô��ڴ��жϣ�ֻΪ�ݴ���
		if (!es_file.toFile().exists())
			return null;

		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);
		InputStream es_is = null;
		InputStream evt_is = null;

		try {// û��nio���Ժ��пոģ����ڵ�loadЧ�ʼ���
			FileChannel es_channel = getFileChannel(es_file.toString());
			es_channel.position(0);
			FileChannel evt_channel = getFileChannel(evt_file.toString());
			evt_channel.position(0);

			es_is = Channels.newInputStream(es_channel);
			evt_is = Channels.newInputStream(evt_channel);

			// ���ؿ���
			DataInputStream es_dis = new DataInputStream(es_is);
			long position = es_dis.readLong();
			ObjectInputStream es_ois = new ObjectInputStream(es_is);
			es = (T) es_ois.readObject();
			es.init();

			// �����¼�
			evt_is.skip(position);
			DataInputStream evt_dis = new DataInputStream(evt_is);
			while (evt_is.available() != 0) {
				byte[] rawData = new byte[evt_dis.readInt()];
				evt_is.read(rawData);
				ByteArrayInputStream bais = new ByteArrayInputStream(rawData);
				ObjectInputStream evt_ois = new ObjectInputStream(bais);
				EventMessage em = (EventMessage) evt_ois.readObject();
				es.apply(em);
			}

			// Ϊʵ���������Ͽ⣬�Ա���ʵ������addEvent����
			es.setRepository(this);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		aggRootCache.put(id, es);
		return es;
	}

	@Override
	public T newInstance(String id) {
		T newEs = creator.apply(id);
		newEs.init();
		newEs.setRepository(this);
		Path es_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);
		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);
		OutputStream os = null;

		try {
			// ����idΨһ�ԣ�û���жϿ����ļ����¼��ļ��Ƿ���ڣ����ǰ������ڴ���ֱ�Ӵ��������ļ�
			es_file.toFile().createNewFile();
			evt_file.toFile().createNewFile();

			// д��ԭʼ����
			os = Channels.newOutputStream(getFileChannel(es_file.toString()));
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeLong(0);
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(newEs);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		aggRootCache.put(id, newEs);
		return newEs;
	}

	@Override
	public List<String> getIds() {
		List<String> list = new ArrayList<String>();
		for (String fileName : this.repositoryStorageDir.toFile().list()) {
			if (fileName.endsWith(SNAPSHOT_SUFFIX)) {
				String id = fileName.substring(0, fileName.length() - SNAPSHOT_SUFFIX.length());
				list.add(id);
			}
		}
		return list;
	}

	@Override
	public void addEvent(String id, EventMessage em) {
		String evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX).toString();

		try {
			fileWritesExecutorService.execute(new Runnable() {
				@Override
				public void run() {
					try {
						// System.out.println(eventMessageByteArrayOutputStream.toByteArray().length);
						//
						// for(byte b :
						// eventMessageByteArrayOutputStream.toByteArray())
						// {
						// System.out.println(String.format("%x", b));
						// }
						eventMessageByteArrayOutputStream.reset();
						eventMessageByteArrayOutputStream.write(SERIALIZE_HEADER);
						eventMessageObjectOutputStream.reset();
						eventMessageObjectOutputStream.writeObject(em);
						eventMessageObjectOutputStream.flush();
						// ObjectOutputStream oos = new
						// ObjectOutputStream(eventMessageByteArrayOutputStream);
						// oos.writeObject(em);
						// oos.flush();
						byte[] rawData = eventMessageByteArrayOutputStream.toByteArray();
						// System.out.println(rawData.length);

						FileChannel channel = getFileChannel(evt_file);
						ByteBuffer buffer = evtFileBufferCache.get(evt_file);

						while (true) {
							if (buffer.remaining() >= Integer.BYTES + rawData.length) {
								buffer.putInt(rawData.length);
								buffer.put(rawData);
								setIdleTimer(evt_file);
								break;
							} else {

								if (buffer.remaining() < Integer.BYTES) {
									buffer.flip();
									channel.write(buffer);
									buffer.clear();
								} else {
									buffer.putInt(rawData.length);

									int offset = 0;
									while (true) {
										if (offset == rawData.length)
											break;
										if (buffer.remaining() == 0) {
											buffer.flip();
											channel.write(buffer);
											buffer.clear();
										} else {
											int len = buffer.remaining() < rawData.length - offset ? buffer.remaining() : rawData.length - offset;
											buffer.put(rawData, offset, len);
											offset += len;
										}
									}

									if (buffer.position() != 0)
										setIdleTimer(evt_file);
									break;
								}
							}
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});

		} catch (RuntimeException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		scheduledService.shutdownNow();
		try {
			fileWritesExecutorService.shutdown();
			// executorService.awaitTermination������1Сʱ����̫������Ϊû����һЩ�־û�����
			fileWritesExecutorService.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			// �ɼ���executorService.shutdownNow()����ȡδ��ɵ�task�б�������Щtask����һ�γ��������ͼ���ִ�С�
			// Ŀǰʱ�����ޣ�������
		}

		for (String key : evtFileBufferCache.keySet()) {
			ByteBuffer buffer = evtFileBufferCache.get(key);
			if (buffer.position() != 0) {
				buffer.flip();
				fileChannelCache.get(key).write(buffer);
				buffer.clear();
			}
		}

		for (String key : aggRootCache.keySet()) {
			AggregateRoot es = aggRootCache.get(key);
			snapshot(es.getId());
		}

		for (FileChannel channel : fileChannelCache.values()) {
			try {
				channel.close();
			} catch (IOException e) {
				// ��¼log������close������˵������̫���ܷ������������ԭ��channel�Ѿ��رա�flushʱ���̿ռ䲻��
			}
		}

		fileChannelCache.clear();
	}

	private void setIdleTimer(String fileName) {
		if (scheduledService.isShutdown())
			return;

		if (!idleTimers.containsKey(fileName)) {
			idleTimers.put(fileName, new IdleScheduledFutureAndRunnableCacheEntry(null, new Runnable() {

				@Override
				public void run() {
					fileWritesExecutorService.execute(new Runnable() {

						@Override
						public void run() {
							ByteBuffer buffer = evtFileBufferCache.get(fileName);

							if (buffer.position() != 0) {
								buffer.flip();
								try {
									fileChannelCache.get(fileName).write(buffer);
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
								buffer.clear();
							}
						}
					});
				}
			}));
		}

		IdleScheduledFutureAndRunnableCacheEntry cacheEntry = idleTimers.get(fileName);

		if (cacheEntry.sf != null && (!cacheEntry.sf.isCancelled() || !cacheEntry.sf.isDone()))
			cacheEntry.sf.cancel(false);
		ScheduledFuture<?> schedule = scheduledService.schedule(cacheEntry.runnable, IDLE_DELAY, TimeUnit.MILLISECONDS);
		cacheEntry.sf = schedule;
	}

	/***
	 * ��ȡ�ļ�channel
	 * <p>
	 * Ϊ�˾��������ļ���open��close������ܵ��£���ˣ�����ļ�û����fileChannelCache������fileChannelCache
	 * ������ֱ�Ӵ�fileChannelCache���ȡ
	 * </p>
	 * <p>
	 * �ļ�io�Ĺر���{@link #close()}��������
	 * </p>
	 * 
	 * @param fileFullName
	 *          �ļ����ƣ�����·������չ��
	 * @return ���ļ���channel
	 */
	private FileChannel getFileChannel(String fileFullName) {

		if (!fileChannelCache.containsKey(fileFullName)) {
			// begin�����ļ�channel
			Path path = FileSystems.getDefault().getPath(fileFullName);
			try {
				FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
				fileChannelCache.put(fileFullName, channel);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			// end�����ļ�channel

			if (fileFullName.endsWith(EVENT_SUFFIX))
				evtFileBufferCache.put(fileFullName, ByteBuffer.allocate(BUFFER_SIZE));
		}

		return fileChannelCache.get(fileFullName);
	}

	private class IdleScheduledFutureAndRunnableCacheEntry {
		public ScheduledFuture<?> sf;
		public Runnable runnable;

		public IdleScheduledFutureAndRunnableCacheEntry(ScheduledFuture<?> sf, Runnable runnable) {
			this.sf = sf;
			this.runnable = runnable;
		}
	}
}
