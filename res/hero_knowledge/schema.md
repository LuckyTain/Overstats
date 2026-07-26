# 英雄知识库 Schema

数据来源：[守望先锋灰机 Wiki](https://overwatch.huijiwiki.com)

## YAML Front Matter 字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| schema_version | int | 是 | 当前为 1 |
| hero_guid | string | 是 | 英文内部名，如 "Ana" |
| hero_name | string | 是 | 灰机 Wiki 正式中文名 |
| hero_name_en | string | 是 | 英文名 |
| aliases | list | 否 | 常用别名 |
| role | string | 是 | 重装 / 输出 / 支援 |
| sub_role | string | 否 | 副职责 |
| archetypes | list | 是 | 玩法原型标签（中文） |
| knowledge_version | string | 是 | YYYY.MM |
| updated_at | string | 是 | YYYY-MM-DD |

## 设计原则

1. 数据驱动，禁止编造无数据支撑的结论。
2. hero_name 必须使用灰机 Wiki 正式中文名称。
3. 评价维度围绕生存、治疗/输出、控制与功能性展开。
