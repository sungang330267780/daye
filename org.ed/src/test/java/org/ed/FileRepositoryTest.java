package org.ed;


import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class FileRepositoryTest {
	final String testDir = "d:/es_test";
	final String name = "≤‚ ‘";

	@Test
	public void testDir() {
		String id = "123";
		FileRepository<SimpleAggregateRoot> fr = new FileRepository<SimpleAggregateRoot>(testDir, name, new Function<String, SimpleAggregateRoot>() {

			@Override
			public SimpleAggregateRoot apply(String id) {
				SimpleAggregateRoot es = new SimpleAggregateRoot();
				es.setId(id);
				return es;
			}

		});

		File f = new File(testDir, name);

		Assert.assertTrue(f.exists());

		SimpleAggregateRoot ses = fr.newInstance(id);

		File esf = new File(f, id + ".snapshot");
		File evtf = new File(f, id + ".evt");
		Assert.assertTrue(esf.exists());
		Assert.assertTrue(evtf.exists());

		SimpleAggregateRoot target = fr.load(id);

		Assert.assertTrue(target.equals(ses));
		Assert.assertTrue(target.getId().equals(ses.getId()));

		ses.add();
		Assert.assertTrue(ses.getI() == 1);

		target = fr.load(id);
		Assert.assertTrue(ses.getI() == target.getI());

		ses.add();
		Assert.assertTrue(ses.getI() == 2);

		target = fr.load(id);
		Assert.assertTrue(ses.getI() == target.getI());

		try {
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@After
	public void clear() {
		deleteDir(new File(testDir));
	}

	private void deleteDir(File dir) {
		if (dir.isDirectory()) {
			for (String file : dir.list()) {
				deleteDir(new File(dir, file));
			}
		}
	}
}
