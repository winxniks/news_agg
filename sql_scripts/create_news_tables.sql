-- Новости и метаданные с версионностью
DROP TABLE IF EXISTS documents CASCADE;
CREATE TABLE documents (
    doc_id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    public_dttm TIMESTAMPTZ, -- Исправлено: TIMESTAMPZ → TIMESTAMPTZ
    eid TEXT, -- id новости на сайте
    doc_url TEXT NOT NULL,
    doc_src varchar(10) NOT NULL,
    doc_text TEXT NOT NULL,
    doc_text_hash TEXT,  -- хеш для дедупликации CHAR(64)
    ver SMALLINT DEFAULT 1,
    valid_from_dttm TIMESTAMPTZ DEFAULT NOW(),
    changed_dttm TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(doc_url, ver)
);

CREATE INDEX IF NOT EXISTS idx_documents_doc_url ON news.documents(doc_url);
CREATE INDEX IF NOT EXISTS idx_documents_doc_text_hash ON news.documents(doc_text_hash);
CREATE INDEX IF NOT EXISTS idx_documents_doc_src ON news.documents(doc_src); ------
CREATE INDEX IF NOT EXISTS idx_documents_public_dttm ON news.documents(public_dttm);
CREATE INDEX IF NOT EXISTS idx_documents_src_public_dttm ON news.documents(doc_src, public_dttm);

-- Чанки текста новостей
DROP TABLE IF EXISTS chunks CASCADE;
CREATE TABLE chunks (
    chunk_id BIGSERIAL PRIMARY KEY,
    doc_id BIGINT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    chunk_idx SMALLINT NOT NULL,          -- порядковый номер чанка в документе
    chunk_text TEXT NOT NULL,              -- текст чанка (для коротких до 500 токенов = весь текст)
    chunk_hash TEXT, -- CHAR(64)
    valid_from_dttm TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(doc_id, chunk_idx)
);

CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON news.chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunks_chunk_hash ON news.chunks(chunk_hash);
CREATE INDEX IF NOT EXISTS idx_chunks_chunk_idx ON news.chunks(chunk_idx);

-- Теги загруженных новостей (изменяется если у тега для конкретного источника новый url)
DROP TABLE IF EXISTS tags CASCADE;
CREATE TABLE tags (
    tag_id BIGSERIAL PRIMARY KEY,
    tag_nm TEXT NOT NULL,
    --tag_eng TEXT,               -- английское название тега с сайта Lenta.ru
    tag_url TEXT,
    tag_src char(10) NOT NULL,
    valid_from_dttm TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tag_nm, tag_src)
);

CREATE INDEX IF NOT EXISTS idx_tags_tag_url ON news.tags(tag_url);
CREATE INDEX IF NOT EXISTS idx_tags_tag_src ON news.tags(tag_src); ---
CREATE INDEX IF NOT EXISTS idx_tags_src_nm ON news.tags(tag_src, tag_nm);

-- Связь новости-теги
DROP TABLE IF EXISTS docs_tags_lnk CASCADE;
CREATE TABLE docs_tags_lnk (
	doc_id BIGINT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    tag_id BIGINT NOT NULL REFERENCES tags(tag_id) ON DELETE CASCADE,
    valid_from_dttm TIMESTAMPTZ DEFAULT NOW(),
    changed_dttm TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tag_id, doc_id)
);

CREATE INDEX IF NOT EXISTS idx_docs_tags_lnk_doc_id ON news.docs_tags_lnk(doc_id);

-- Комментарии
COMMENT ON TABLE documents IS 'Новости и метаданные';
COMMENT ON TABLE chunks IS 'Чанки текста новостей';
COMMENT ON TABLE tags IS 'Теги новостей из источников';
COMMENT ON TABLE docs_tags_lnk IS 'Связь новости-теги';