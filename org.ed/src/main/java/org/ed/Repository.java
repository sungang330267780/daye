package org.ed;

import java.io.Closeable;
import java.util.List;

/***
 * 业务资料库接口
 * <p>
 * 做业务事件的存储，并可以回溯业务实体
 * </p>
 * <p>
 * 根据此接口可做文件资料库、数据库资料库、nosql资料库等
 * </p>
 * 
 * @author tao
 *
 * @param <T>
 */
public interface Repository<T extends AggregateRoot> extends Closeable {
	/***
	 * 为业务做快照
	 * <p>
	 * 快照应该有策略的做，尽量避免做快照的频率，可想到的：
	 * <ul>
	 * <li>1、按时间频率</li>
	 * <li>2、按事件测出发次数</li>
	 * </ul>
	 * 快照可以加快业务{@link #load(String)}的速度
	 * </p>
	 * 
	 * @param id
	 *            业务id
	 */
	void snapshot(String id);

	/***
	 * 加载业务
	 * <p>
	 * 加载业务要做两件事：
	 * <ul>
	 * <li>1、读取快照做业务实例</li>
	 * <li>2、加载快照以后的事件并应用的业务实体</li>
	 * </ul>
	 * <p>
	 * 
	 * @param id
	 *            业务id
	 * @return 业务实例
	 */
	T load(String id);

	/***
	 * 创建新业务实例
	 * <p>
	 * 当有新业务时，首先通过此方法创建业务实例，然后再做业务
	 * </p>
	 * 
	 * @param id
	 *            业务id
	 * @return 新业务实例
	 */
	T newInstance(String id);

	/***
	 * 获取当前资料库中全部业务实例id
	 * 
	 * @return 业务id列表
	 */
	List<String> getIds();

	/***
	 * 添加业务事件
	 * <p>
	 * 根据业务id添加并记录事件
	 * </p>
	 * <p>
	 * <b>此方法不该由应用调用。它是给{@link AggregateRoot#apply(EventMessage)}特定的接口</b>
	 * </p>
	 * 
	 * @param id
	 *            业务id
	 * @param em
	 *            事件消息
	 */
	void addEvent(String id, EventMessage em);
}
