package org.ed;

import java.io.Closeable;
import java.util.List;

/***
 * ҵ�����Ͽ�ӿ�
 * <p>
 * ��ҵ���¼��Ĵ洢�������Ի���ҵ��ʵ��
 * </p>
 * <p>
 * ���ݴ˽ӿڿ����ļ����Ͽ⡢���ݿ����Ͽ⡢nosql���Ͽ��
 * </p>
 * 
 * @author tao
 *
 * @param <T>
 */
public interface Repository<T extends AggregateRoot> extends Closeable {
	/***
	 * Ϊҵ��������
	 * <p>
	 * ����Ӧ���в��Ե������������������յ�Ƶ�ʣ����뵽�ģ�
	 * <ul>
	 * <li>1����ʱ��Ƶ��</li>
	 * <li>2�����¼����������</li>
	 * </ul>
	 * ���տ��Լӿ�ҵ��{@link #load(String)}���ٶ�
	 * </p>
	 * 
	 * @param id
	 *            ҵ��id
	 */
	void snapshot(String id);

	/***
	 * ����ҵ��
	 * <p>
	 * ����ҵ��Ҫ�������£�
	 * <ul>
	 * <li>1����ȡ������ҵ��ʵ��</li>
	 * <li>2�����ؿ����Ժ���¼���Ӧ�õ�ҵ��ʵ��</li>
	 * </ul>
	 * <p>
	 * 
	 * @param id
	 *            ҵ��id
	 * @return ҵ��ʵ��
	 */
	T load(String id);

	/***
	 * ������ҵ��ʵ��
	 * <p>
	 * ������ҵ��ʱ������ͨ���˷�������ҵ��ʵ����Ȼ������ҵ��
	 * </p>
	 * 
	 * @param id
	 *            ҵ��id
	 * @return ��ҵ��ʵ��
	 */
	T newInstance(String id);

	/***
	 * ��ȡ��ǰ���Ͽ���ȫ��ҵ��ʵ��id
	 * 
	 * @return ҵ��id�б�
	 */
	List<String> getIds();

	/***
	 * ���ҵ���¼�
	 * <p>
	 * ����ҵ��id��Ӳ���¼�¼�
	 * </p>
	 * <p>
	 * <b>�˷���������Ӧ�õ��á����Ǹ�{@link AggregateRoot#apply(EventMessage)}�ض��Ľӿ�</b>
	 * </p>
	 * 
	 * @param id
	 *            ҵ��id
	 * @param em
	 *            �¼���Ϣ
	 */
	void addEvent(String id, EventMessage em);
}
