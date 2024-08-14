Wymagania
========

Do poprawnego działania projektu wymagany jest Docker oraz Astro CLI.

Zawartość projektu
================

Projekt Astro zawiera następujące pliki i foldery:

- dags: Ten folder zawiera pliki Pythona dla DAGów Airflow:
    - `example_astronauts`: Ten DAG pokazuje prosty przykład potoku ETL, który odpytuje listę astronautów znajdujących się obecnie w kosmosie z Open Notify API i drukuje oświadczenie dla każdego astronauty. DAG wykorzystuje interfejs API TaskFlow do definiowania zadań w Pythonie i dynamicznego mapowania zadań w celu dynamicznego drukowania instrukcji dla każdego astronauty. Więcej informacji na temat działania tego DAG można znaleźć w naszym samouczku [Pierwsze kroki] (https://docs.astronomer.io/learn/get-started-with-airflow).
    - `notaries`: Właściwy DAG dla projektu. Pobiera, transoformuje oraz ładuje dane do wskazanej w połączeniach bazy.
- Dockerfile: Ten plik zawiera wersjonowany obraz Astro Runtime Docker, który zapewnia zróżnicowane środowisko Airflow. Jeśli chcesz wykonać inne polecenia lub nadpisania w czasie wykonywania, określ je tutaj.
- include: Ten folder zawiera wszelkie dodatkowe pliki, które chcesz dołączyć jako część swojego projektu.
- packages.txt: Zainstaluj pakiety na poziomie systemu operacyjnego potrzebne do projektu, dodając je do tego pliku.
- requirements.txt: Zainstaluj pakiety Pythona potrzebne do projektu, dodając je do tego pliku.
- plugins: Dodaj niestandardowe lub społecznościowe wtyczki dla swojego projektu do tego pliku.

Hostowanie projektu lokalnie
===========================

1. Uruchom Airflow na lokalnym komputerze, uruchamiając "astro dev start".

To polecenie uruchomi 5 kontenerów Docker na komputerze, każdy dla innego komponentu Airflow:

- Postgres: Baza danych metadanych Airflow
- Webserver: Komponent Airflow odpowiedzialny za renderowanie interfejsu użytkownika Airflow
- Scheduler: Komponent Airflow odpowiedzialny za monitorowanie i wyzwalanie zadań
- Triggerer: Komponent Airflow odpowiedzialny za wyzwalanie odroczonych zadań
- MinIO: System przechowywania plików

2. Sprawdź, czy wszystkie 5 kontenerów Docker zostały poprawnie utworzone, uruchamiając "docker ps".

Uwaga: Uruchomienie "astro dev start" uruchomi projekt z serwerem internetowym Airflow na porcie 8080, Postgres na porcie 5432 i MinIO na porcie 9001. Jeśli masz już przydzielony jeden z tych portów, możesz zatrzymać istniejące kontenery Docker lub zmienić port. (https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Przejdź do interfejsu użytkownika dla lokalnego projektu Airflow. Aby to zrobić, przejdź do http://localhost:8080/ i zaloguj się za pomocą "admin" zarówno jako nazwy użytkownika, jak i hasła.

Powinieneś także mieć dostęp do swojej bazy danych Postgres pod adresem "localhost:5432/postgres".
