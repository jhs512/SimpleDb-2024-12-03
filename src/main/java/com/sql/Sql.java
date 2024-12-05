package com.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.simpleDb.SimpleDb;

public class Sql {
	private StringBuilder sb = new StringBuilder();
	private List<Object> params = new ArrayList<>();
	private final SimpleDb simpleDb;

	public Sql(SimpleDb simpleDb) {
		this.simpleDb = simpleDb;
	}

	public Sql append(String sql, Object... params) {
		sb.append(sql + " ");
		Collections.addAll(this.params, params);

		return this;
	}

	public Sql appendIn(String sql, Object... params) {
		String[] split = sql.split("\\?");
		sb.append(split[0]);

		for (int i = 0; i < params.length; i++) {
			sb.append("?,");
		}

		sb.deleteCharAt(sb.length() - 1);
		sb.append(") ");

		for (Object param : params) {
			this.params.add(param);
		}

		return this;
	}

	public long insert() {
		long id = simpleDb.runInsert(sb.toString(), params);
		return id;
	}

	public long update() {
		return simpleDb.runUpdate(sb.toString(), params);
	}

	public List<Map<String, Object>> selectRows() {
		return simpleDb.runSelectRows(sb.toString(), params);
	}

	public <T>List<T> selectRows(Class<T> clazz) {
		return simpleDb.runSelectRows(sb.toString(), clazz);
	}

	public Map<String, Object> selectRow() {
		return simpleDb.runSelectRow(sb.toString(), params);
	}

	public <T>T selectRow(Class<T> clazz) {
		return simpleDb.runSelectRow(sb.toString(), clazz);
	}
	public LocalDateTime selectDatetime() {
		return simpleDb.runSelectDatetime(sb.toString(), params);
	}

	public Long selectLong() {
		return simpleDb.runSelectLong(sb.toString(), params);
	}

	public String selectString() {
		return simpleDb.runSelectString(sb.toString(), params);
	}

	public Boolean selectBoolean() {
		return simpleDb.runSelectBoolean(sb.toString(), params);
	}

	public List<Long> selectLongs() {
		return simpleDb.runSelectLongs(sb.toString(), params);
	}
}
