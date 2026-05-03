import asyncio
import aiohttp
import logging
import os
import json
from typing import List, Dict, Optional, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

class VKAPIParser:
    """
    Асинхронный клиент для сбора данных пользователей VK через официальный API.
    Реализует фильтрацию по списку ФИО, городу и дате рождения с учётом
    квот платформы, пагинации и нормализации данных.
    """
    API_VERSION = "5.131"
    BASE_URL = "https://api.vk.com/method"
    DEFAULT_COUNTRY_ID = 1  # Россия

    def __init__(self, access_token: str, requests_per_second: float = 3.0):
        if not access_token or access_token == "your_token_here":
            raise ValueError("Требуется действительный VK Access Token.")
        self.token = access_token
        self.rps = requests_per_second
        self.semaphore = asyncio.Semaphore(int(max(1, requests_per_second)))
        self.request_delay = 1.0 / requests_per_second
        self._session: Optional[aiohttp.ClientSession] = None
        self._city_cache: Dict[str, int] = {}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "VKDataParser/1.0"}
            )
        return self._session

    async def _execute_request(
        self,
        method: str,
        params: Dict[str, Any],
        max_retries: int = 3
    ) -> Dict[str, Any]:
        req_params = {**params, "access_token": self.token, "v": self.API_VERSION}
        url = f"{self.BASE_URL}/{method}"

        for attempt in range(max_retries):
            async with self.semaphore:
                session = await self._ensure_session()
                async with session.get(url, params=req_params) as response:
                    data = await response.json()

                    if "error" in data:
                        err = data["error"]
                        code = err.get("error_code")
                        msg = err.get("error_msg", "Unknown error")

                        if code == 6:  # Too many requests per second
                            backoff = err.get("retry_after", 2 ** attempt)
                            logger.warning(f"Rate limit exceeded (Error 6). Backoff: {backoff}s")
                            await asyncio.sleep(backoff)
                            continue
                        elif code == 5:
                            raise ValueError("Недействительный или истёкший access_token.")
                        elif code == 14:
                            raise RuntimeError("Требуется капча. Доступ временно заблокирован.")
                        elif code == 9:
                            logger.warning("Флуд-контроль. Увеличение задержки.")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            raise RuntimeError(f"VK API Error [{code}]: {msg}")

                    return data.get("response", {})

            await asyncio.sleep(self.request_delay)

        raise RuntimeError(f"Превышено максимальное количество попыток ({max_retries}) для метода {method}")

    async def resolve_city_id(self, city_name: str, country_id: int = DEFAULT_COUNTRY_ID) -> int:
        key = city_name.strip().lower()
        if key in self._city_cache:
            return self._city_cache[key]

        logger.info(f"Поиск идентификатора города: {city_name}")
        response = await self._execute_request("database.getCities", {"country_id": country_id, "q": key, "count": 1})
        items = response.get("items", [])

        if not items:
            raise ValueError(f"Город '{city_name}' не найден в справочнике VK (country_id={country_id}).")

        city_id = items[0]["id"]
        self._city_cache[key] = city_id
        logger.info(f"Город '{city_name}' -> ID: {city_id}")
        return city_id

    async def search_users(self, query: str, city_id: int, max_results: int = 1000) -> List[Dict[str, Any]]:
        """
        Поиск пользователей с пагинацией. VK API ограничивает суммарную выборку
        методом users.search до ~1000 записей на один поисковый запрос.
        """
        params = {
            "q": query,
            "city": city_id,
            "count": 100,
            "fields": "bdate,city",
            "sort": 0
        }
        collected: List[Dict[str, Any]] = []
        offset = 0

        while True:
            params["offset"] = offset
            response = await self._execute_request("users.search", params)
            total_available = response.get("count", 0)
            items = response.get("items", [])

            if not items:
                break

            collected.extend(items)
            offset += len(items)
            logger.info(f"Запрос '{query}': загружено {len(collected)}/{min(total_available, max_results)}")

            if len(collected) >= total_available or len(collected) >= max_results:
                break

        return collected[:max_results]

    @staticmethod
    def filter_by_birthdate(users: List[Dict[str, Any]], target_date: str) -> List[Dict[str, Any]]:
        """
        Постобработка результатов по дате рождения.
        Поддерживаемые форматы: DD.MM.YYYY или DD.MM
        """
        parts = target_date.strip().split(".")
        if not (2 <= len(parts) <= 3):
            raise ValueError("Некорректный формат даты. Требуется DD.MM или DD.MM.YYYY")

        t_day, t_month = parts[0].zfill(2), parts[1].zfill(2)
        t_year = parts[2] if len(parts) == 3 else None

        filtered = []
        for user in users:
            bdate_str = user.get("bdate", "").strip()
            if not bdate_str:
                continue

            d_parts = bdate_str.split(".")
            if len(d_parts) == 2:
                d_parts.append(None)
            elif len(d_parts) != 3:
                continue

            d_day = d_parts[0].zfill(2)
            d_month = d_parts[1].zfill(2)
            d_year = d_parts[2]

            if d_day == t_day and d_month == t_month:
                if t_year is None or d_year == t_year:
                    filtered.append(user)

        return filtered

    async def run(
        self,
        name_list: List[str],
        city: str,
        birthdate: str,
        output_file: str = "filtered_vk_users.json"
    ) -> List[Dict[str, Any]]:
        logger.info("Запуск процесса сбора данных VK API...")
        try:
            city_id = await self.resolve_city_id(city)
        except ValueError as e:
            logger.error(str(e))
            return []

        all_results: List[Dict[str, Any]] = []
        for idx, name in enumerate(name_list, 1):
            logger.info(f"Обработка запроса {idx}/{len(name_list)}: '{name}'")
            try:
                raw_users = await self.search_users(name, city_id)
                matched = self.filter_by_birthdate(raw_users, birthdate)
                all_results.extend(matched)
            except Exception as e:
                logger.error(f"Ошибка при обработке имени '{name}': {e}")
                continue

            if idx < len(name_list):
                await asyncio.sleep(1.0)

        unique_users = {u["id"]: u for u in all_results}
        final_list = list(unique_users.values())
        logger.info(f"Сбор завершён. Уникальных пользователей: {len(final_list)}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, indent=2)
        logger.info(f"Данные экспортированы в {output_file}")

        return final_list

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("HTTP-сессия закрыта.")

async def main():
    TOKEN = os.getenv("VK_ACCESS_TOKEN", "your_token_here")
    SEARCH_NAMES = ["Иванов Иван", "Петров Петр", "Сидорова Анна"]
    TARGET_CITY = "Москва"
    TARGET_BIRTHDATE = "15.04.1995"  # Или "15.04" для фильтрации без учёта года

    parser = VKAPIParser(access_token=TOKEN, requests_per_second=3.0)
    try:
        await parser.run(
            name_list=SEARCH_NAMES,
            city=TARGET_CITY,
            birthdate=TARGET_BIRTHDATE,
            output_file="vk_users_export.json"
        )
    except KeyboardInterrupt:
        logger.info("Процесс прерван пользователем.")
    except Exception as e:
        logger.error(f"Критическая ошибка выполнения: {e}")
    finally:
        await parser.close()

if __name__ == "__main__":
    asyncio.run(main())