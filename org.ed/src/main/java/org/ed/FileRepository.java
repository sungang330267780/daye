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
 * 文件资料库
 * 
 * <p>
 * 文档结构和命名规则:
 * <ul>
 * <li>
 * {repositoryStorageDir}/{name}为此资料库的存储目录(repositoryStorageDir和name为构造函数传入的参数
 * {@link #FileRepository(String, String, Function)})</li>
 * <li>
 * {repositoryStorageDir}/{name}/*.snapshot为资料库的所有快照文件</li>
 * <li>
 * {repositoryStorageDir}/{name}/*.evt为资料库的所有事件文件</li>
 * <li>
 * 快照的命名规则为{id}.snapshot；同理，事件命名规则为{id}.evt</li>
 * <li>每个业务实体的id是唯一的</li>
 * </ul>
 * </p>
 * <p>
 * 快照文件结构：
 * <ul>
 * <li>{事件文件偏移量}{业务实例序列化数据}</li>
 * <li>只有一个业务实体</li>
 * </ul>
 * </p>
 * <p>
 * 事件文件结构：
 * <ul>
 * <li>{序列化数据大小}{事件消息实体序列化数据}</li>
 * <li>该文件保持业务实体保持的所有事件</li>
 * </ul>
 * </p>
 * 
 * @author tao
 *
 * @param <T>
 */
public class FileRepository<T extends AggregateRoot> implements Repository<T>, Closeable {

	/***
	 * 快照文件后缀
	 */
	private final String SNAPSHOT_SUFFIX = ".snapshot";
	/***
	 * 事件文件后缀
	 */
	private final String EVENT_SUFFIX = ".evt";
	/***
	 * 缓存大小，在自己的机器上测试1M为最快，大于1M没作用
	 */
	private final int BUFFER_SIZE = 1024 * 1024;
	/***
	 * 空闲延迟时间，单位：毫秒
	 */
	private final int IDLE_DELAY = 100;
	/***
	 * 为了重复用ObjectOutputStream不得已，以后找好的解决方法
	 */
	private final byte[] SERIALIZE_HEADER = new byte[] { (byte) 0xac, (byte) 0xed, 0x00, 0x05 };
	/***
	 * 业务实例构造器
	 */
	private Function<String, T> creator;
	/***
	 * 资料库存储目录
	 */
	private Path repositoryStorageDir;
	/***
	 * 文件channel缓存
	 */
	private Map<String, FileChannel> fileChannelCache = new HashMap<String, FileChannel>();
	/***
	 * 业务实体缓存
	 */
	private Map<String, AggregateRoot> aggRootCache = new HashMap<String, AggregateRoot>();
	/***
	 * 文件写操作线程
	 */
	private ExecutorService fileWritesExecutorService;
	/***
	 * 事件文件buffer，为了加快速度
	 */
	private Map<String, ByteBuffer> evtFileBufferCache = new HashMap<String, ByteBuffer>();
	/***
	 * 事件字节流
	 */
	private ByteArrayOutputStream eventMessageByteArrayOutputStream;
	/***
	 * 事件序列化流
	 */
	private ObjectOutputStream eventMessageObjectOutputStream;

	private Map<String, IdleScheduledFutureAndRunnableCacheEntry> idleTimers = new HashMap<String, IdleScheduledFutureAndRunnableCacheEntry>();
	private ScheduledExecutorService scheduledService;

	/***
	 * 文件资料库构造函数
	 * 
	 * @param repositoryStorageDir
	 *          资料库存储目录
	 * @param name
	 *          业务名称
	 * @param creator
	 *          业务实例构造器
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
				// begin清理以前的快照
				if (fileChannelCache.containsKey(file.getPath())) {
					fileChannelCache.get(file.getPath()).close();
					fileChannelCache.remove(file.getPath());
				}

				file.delete();
				// end清理以前的快照

				// begin重新建立快照
				file.createNewFile();
				os = Channels.newOutputStream(getFileChannel(file.getPath()));
				file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX).toFile();
				long positing = file.length();
				DataOutputStream dos = new DataOutputStream(os);
				dos.writeLong(positing);
				ObjectOutputStream oos = new ObjectOutputStream(dos);
				oos.writeObject(es);
				// end重现建立快照

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T load(String id) {
		// 为了减少业务实例
		if (aggRootCache.containsKey(id))
			return (T) aggRootCache.get(id);

		T es = null;

		Path es_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);

		// 如果id是从getIds方法获取，不应该存在此判断，只为容错处理
		if (!es_file.toFile().exists())
			return null;

		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);
		InputStream es_is = null;
		InputStream evt_is = null;

		try {// 没用nio，以后有空改，现在的load效率极低
			FileChannel es_channel = getFileChannel(es_file.toString());
			es_channel.position(0);
			FileChannel evt_channel = getFileChannel(evt_file.toString());
			evt_channel.position(0);

			es_is = Channels.newInputStream(es_channel);
			evt_is = Channels.newInputStream(evt_channel);

			// 加载快照
			DataInputStream es_dis = new DataInputStream(es_is);
			long position = es_dis.readLong();
			ObjectInputStream es_ois = new ObjectInputStream(es_is);
			es = (T) es_ois.readObject();
			es.init();

			// 加载事件
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

			// 为实体设置资料库，以便于实体库调用addEvent方法
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
			// 由于id唯一性，没有判断快照文件和事件文件是否存在，而是按不存在处理，直接创建两个文件
			es_file.toFile().createNewFile();
			evt_file.toFile().createNewFile();

			// 写入原始快照
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
			// executorService.awaitTermination设置了1小时，不太合理，因为没有做一些持久化处理
			fileWritesExecutorService.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			// 可加入executorService.shutdownNow()，获取未完成的task列表，保持这些task，下一次程序启动就继续执行。
			// 目前时间有限，待完善
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
				// 记录log，根据close函数的说明，不太可能发生，猜想可能原因：channel已经关闭、flush时磁盘空间不足
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
	 * 获取文件channel
	 * <p>
	 * 为了尽量避免文件的open和close造成性能低下，因此：如果文件没有在fileChannelCache里，则加入fileChannelCache
	 * ，否则直接从fileChannelCache里获取
	 * </p>
	 * <p>
	 * 文件io的关闭由{@link #close()}方法负责
	 * </p>
	 * 
	 * @param fileFullName
	 *          文件名称，包括路径和扩展名
	 * @return 该文件的channel
	 */
	private FileChannel getFileChannel(String fileFullName) {

		if (!fileChannelCache.containsKey(fileFullName)) {
			// begin加载文件channel
			Path path = FileSystems.getDefault().getPath(fileFullName);
			try {
				FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
				fileChannelCache.put(fileFullName, channel);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			// end加载文件channel

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
