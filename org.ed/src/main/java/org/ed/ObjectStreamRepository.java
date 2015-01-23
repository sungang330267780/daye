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
	 * �����ļ���׺
	 */
	private final String SNAPSHOT_SUFFIX = ".snapshot";
	/***
	 * �¼��ļ���׺
	 */
	private final String EVENT_SUFFIX = ".evt";
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
	private Map<String, FileChannel> fileChannelCache;
	/**
	 * �������������
	 */
	private Map<FileChannel, FSTObjectOutput> objectOutputCache;
	/***
	 * ҵ��ʵ�建��
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
				// begin���½�������
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
				// end���ֽ�������

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
		// Ϊ�˼���ҵ��ʵ��
		if (aggRootCache.containsKey(id))
			return (T) aggRootCache.get(id);

		T agg = null;

		Path snap_file = this.repositoryStorageDir.resolve(id + SNAPSHOT_SUFFIX);

		// ���id�Ǵ�getIds������ȡ����Ӧ�ô��ڴ��жϣ�ֻΪ�ݴ���
		if (!snap_file.toFile().exists())
			return null;

		Path evt_file = this.repositoryStorageDir.resolve(id + EVENT_SUFFIX);

		FileChannel snap_channel = null;
		FSTObjectInput snap_fstoi = null;
		try {// û��nio���Ժ��пոģ����ڵ�loadЧ�ʼ���
			snap_channel = FileChannel.open(snap_file, StandardOpenOption.READ);
			FileChannel evt_channel = getFileChannel(evt_file.toString());

			// ���ؿ���
			ByteBuffer positionBytes = ByteBuffer.allocate(Long.BYTES);
			snap_channel.read(positionBytes);
			positionBytes.flip();
			long position = positionBytes.getLong();
			evt_channel.position(position);

			snap_fstoi = new FSTObjectInput(Channels.newInputStream(snap_channel));
			agg = (T) snap_fstoi.readObject();
			agg.init();

			// �����¼�
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
			// Ϊʵ���������Ͽ⣬�Ա���ʵ������addEvent����
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
			// ����idΨһ�ԣ�û���жϿ����ļ����¼��ļ��Ƿ���ڣ����ǰ������ڴ���ֱ�Ӵ��������ļ�
			snap_file.toFile().createNewFile();
			evt_file.toFile().createNewFile();

			// д��ԭʼ����
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
	 *            �ļ����ƣ�����·������չ��
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
