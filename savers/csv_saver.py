import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CSVSaver:
    """Класс для сохранения данных"""
    
    @staticmethod
    def save_to_csv(news_data, filename=None):
        """Сохраняет новости в CSV файл"""
        if not news_data:
            logger.warning("Нет данных для сохранения")
            return None
        
        df = pd.DataFrame(news_data)
        
        if "datetime" in df.columns:
            df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        if filename is None:
            filename = CSVSaver._get_default_filename()
        
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        logger.info(f"Данные сохранены в файл: {filename}, всего записей: {len(df)}")
        return df
    
    @staticmethod
    def _get_default_filename():
        """Возвращает имя файла по умолчанию"""
        from datetime import datetime
        return rf"C:\Users\Пользователь\Documents\Универ_мага\диплом\news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    @staticmethod
    def filter_valid_news(news_list):
        """Фильтрует новости с пустым content"""
        valid = [item for item in news_list if item.get("content") and item.get("content").strip()]
        invalid_count = len(news_list) - len(valid)
        if invalid_count:
            logger.warning(f"Новостей с пустым content: {invalid_count}")
        return valid