package com.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
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

	public long insert() {
		long id = simpleDb.runInsert(sb.toString(), params);
		return id;
	}

	public long update() {
		return simpleDb.runUpdate(sb.toString(), params);
	}

	public List<Map<String, Object>> selectRows() {
		return simpleDb.runSelectRows(sb.toString());
	}

	public Map<String, Object> selectRow() {
		return simpleDb.runSelectRow(sb.toString());
	}

	public LocalDateTime selectDatetime() {
		return simpleDb.runSelectDatetime(sb.toString());
	}

	public Long selectLong() {
		return simpleDb.runSelectLong(sb.toString());
	}

	public String selectString() {
		return simpleDb.runSelectString(sb.toString());
	}

	public Boolean selectBoolean() {
		return simpleDb.runSelectBoolean(sb.toString());
	}
}
