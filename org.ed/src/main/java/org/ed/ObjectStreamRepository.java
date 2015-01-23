package org.ed;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import java.util.function.Function;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

public class ObjectStreamRepository<T extends AggregateRoot> implements Repository<T>, Closeable {
	/***
	 * 快照文件后缀
	 */
	private final String SNAPSHOT_SUFFIX = ".snapshot";
	/***
	 * 事件文件后缀
	 */
	private final String EVENT_SUFFIX = ".evt";
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
	private Map<String, FileChannel> fileChannelCache;
	/**
	 * 对象输出流缓存
	 */
	private Map<FileChannel, FSTObjectOutput> objectOutputCache;
	/***
	 * 业务实体缓存
	 */
	private Map<String, AggregateRoot> aggRootCache;
	private EventBus eventBus;

	public ObjectStreamRepository(String repositoryStorageDir, String name, Function<String, T> creator, EventBus eventBus) {
		this.eventBus = eventBus;
		this.repositoryStorageDir = FileSystems.getDefault().getPath(repositoryStorageDir, name);
		this.creator = creator;
		this.eventBus = eventBus;

		fileChannelCache = new HashMap<String, FileChannel>();
		aggRootCache = new HashMap<String, AggregateRoot>();
		objectOutputCache = new HashMap<FileChannel, FSTObjectOutput>();
		if (!Files.exists(this.repositoryStorageDir))
			try {
				Files.createDirectories(this.repositoryStorageDir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}

	@Override
	public void snapshot(String id) {
		File file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX).toFile();

		if (file.exists()) {
			T es = load(id);

			FileChannel snap_channel = null;
			FSTObjectOutput snap_fstoo = null;
			try {

				file.delete();
				// begin重新建立快照
				file.createNewFile();
				snap_channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE);
				snap_fstoo = new FSTObjectOutput(Channels.newOutputStream(snap_channel));

				String evt_fileFullName = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX).toString();
				FileChannel evt_channel = getFileChannel(evt_fileFullName);
				getObjectOutput(evt_channel).flush();
				long position = evt_channel.size();

				ByteBuffer positionBytes = ByteBuffer.allocate(Long.BYTES);
				positionBytes.putLong(position);
				positionBytes.flip();
				snap_channel.write(positionBytes);
				snap_fstoo.writeObject(es);
				// end重现建立快照

			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					if (snap_fstoo != null)
						snap_fstoo.close();

					if (snap_channel != null && snap_channel.isOpen())
						snap_channel.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T load(String id) {
		// 为了减少业务实例
		if (aggRootCache.containsKey(id))
			return (T) aggRootCache.get(id);

		T agg = null;

		Path snap_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);

		// 如果id是从getIds方法获取，不应该存在此判断，只为容错处理
		if (!snap_file.toFile().exists())
			return null;

		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);

		FileChannel snap_channel = null;
		FSTObjectInput snap_fstoi = null;
		try {// 没用nio，以后有空改，现在的load效率极低
			snap_channel = FileChannel.open(snap_file, StandardOpenOption.READ);
			FileChannel evt_channel = getFileChannel(evt_file.toString());

			// 加载快照
			ByteBuffer positionBytes = ByteBuffer.allocate(Long.BYTES);
			snap_channel.read(positionBytes);
			positionBytes.flip();
			long position = positionBytes.getLong();
			evt_channel.position(position);

			snap_fstoi = new FSTObjectInput(Channels.newInputStream(snap_channel));
			agg = (T) snap_fstoi.readObject();
			agg.init();

			// 加载事件
			FSTObjectInput evt_fstoi = new FSTObjectInput();

			int count = 0;
			while (position != evt_channel.size()) {

				if (count % 10000 == 0) {
					position += evt_fstoi.getCodec().getInputPos();
					evt_channel.position(position);
					evt_fstoi.resetForReuse(Channels.newInputStream(evt_channel));
				}
				EventMessage em = (EventMessage) evt_fstoi.readObject();
				count++;
				agg.apply(em);

			}
			evt_fstoi.close();
			// 为实体设置资料库，以便于实体库调用addEvent方法
			agg.setRepository(this);
			agg.setEventBus(eventBus);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (snap_fstoi != null)
					snap_fstoi.close();

				if (snap_channel != null && snap_channel.isOpen())
					snap_channel.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		aggRootCache.put(id, agg);
		return agg;
	}

	@Override
	public T newInstance(String id) {
		T newAgg = creator.apply(id);
		newAgg.init();
		newAgg.setRepository(this);
		newAgg.setEventBus(eventBus);
		Path snap_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);
		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);

		FileChannel snap_channel = null;
		FSTObjectOutput snap_fstoo = null;
		try {
			// 由于id唯一性，没有判断快照文件和事件文件是否存在，而是按不存在处理，直接创建两个文件
			snap_file.toFile().createNewFile();
			evt_file.toFile().createNewFile();

			// 写入原始快照
			snap_channel = FileChannel.open(snap_file, StandardOpenOption.WRITE);
			ByteBuffer positionBytes = ByteBuffer.allocate(Long.BYTES);
			snap_channel.write(positionBytes);
			snap_fstoo = new FSTObjectOutput(Channels.newOutputStream(snap_channel));
			snap_fstoo.writeObject(newAgg);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (snap_fstoo != null)
					snap_fstoo.close();

				if (snap_channel != null && snap_channel.isOpen())
					snap_channel.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		aggRootCache.put(id, newAgg);
		return newAgg;
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

	int i = 0;

	@Override
	public void addEvent(String id, EventMessage em) {
		String evt_file = this.repositoryStorageDir.toString() + "\\" + id + EVENT_SUFFIX;

		FSTObjectOutput fstoo = getObjectOutput(getFileChannel(evt_file));
		try {
			fstoo.writeObject(em);
			i++;
			if (i % 1000 == 0)
				fstoo.flush();
		} catch (IOException e) {
		}
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
	 *            文件名称，包括路径和扩展名
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
		}

		return fileChannelCache.get(fileFullName);
	}

	private FSTObjectOutput getObjectOutput(FileChannel channel) {
		if (!objectOutputCache.containsKey(channel)) {
			objectOutputCache.put(channel, new FSTObjectOutput(Channels.newOutputStream(channel)));
		}

		return objectOutputCache.get(channel);
	}

	@Override
	public void close() throws IOException {

		for (String id : aggRootCache.keySet()) {
			snapshot(id);
		}

		for (FSTObjectOutput foo : objectOutputCache.values()) {
			foo.close();
		}

		for (FileChannel channel : fileChannelCache.values()) {
			channel.close();
		}

		if (eventBus != null)
			eventBus.close();
	}
}
