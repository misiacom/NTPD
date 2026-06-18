# Sprawozdanie lab12
Wiktoria Cendrowswka-Kociuga Gr3

## Zadanie 1 
### Obrazy
metabase/metabase:latest   4cf7d161c4f3       1.72GB          738MB    U   
postgres:16                081f1bc7bd5e        642MB          166MB    U  

### Uruchomienie środowiska

docker compose up -d


### Uruchomione kontenery

- postgres_bi
- metabase_bi

### Dostęp do Metabase

http://localhost:3000

## Zadanie 2

Połączyłam się z bazą i wyświetliłam dane czego wyniki można zobaczyć na załączonym zrzucie ekranu

## Zadanie 3

### Pytanie 1 – liczba transakcji według statusu

utworzone przy użyciu wizualnego kreatora zapytań Metabase.

Wizualizacja: wykres słupkowy.

Uzasadnienie:
Wykres słupkowy pozwala łatwo porównać liczbę transakcji pomiędzy poszczególnymi statusami (paid, pending, cancelled).

### Pytanie 2 – liczba transakcji i suma wartości według kategorii

Wykorzystałam agregację danych według kategorii produktu

Wizualizacja: wykres słupkowy.

Uzasadnienie:
Wykres słupkowy umożliwia szybkie porównanie przychodów generowanych przez poszczególne kategorie produktów.

### Pytanie 3 – przychód dla opłaconych transakcji

Zapytanie zostało wykonane przy użyciu SQL.

```sql
SELECT
    category,
    COUNT(*) AS events,
    SUM(amount) AS revenue
FROM transactions
WHERE status = 'paid'
GROUP BY category
ORDER BY revenue DESC;
```

Wizualizacja: wykres kołowy.

Uzasadnienie:
Wykres kołowy dobrze prezentuje procentowy udział poszczególnych kategorii w całkowitym przychodzie.

## Zadanie 4

Wyniki są na załączonym zrzucie ekranu

## Zadanie 5

### Analiza biznesowa

Przeprowadziłam analizę przychodów według kategorii produktów.

Największy przychód generowała kategoria elektronika, co wynika z wysokiej wartości pojedynczych transakcji. Kategorie książki, odzież oraz sport generowały mniejsze przychody, jednak charakteryzowały się większą liczbą transakcji o niższej wartości.

Analiza pozwala stwierdzić, że największy wpływ na całkowity przychód mają produkty o wysokiej cenie jednostkowej, nawet jeśli liczba transakcji jest mniejsza niż w pozostałych kategoriach.

### Eksport pliku 

Eksportowzałam plik Revenue by Category do csv co uwieczniłam na załączonym zrzucie ekranu

## Porównanie pojęć

### Przetwarzanie danych a Business Intelligence

Przetwarzanie danych polega na pozyskiwaniu, czyszczeniu, transformacji i przygotowaniu danych do dalszej analizy. Business Intelligence wykorzystuje przygotowane dane do tworzenia raportów, wizualizacji oraz wspomagania podejmowania decyzji biznesowych.

### Dashboard a raport statyczny

Dashboard jest interaktywnym narzędziem umożliwiającym filtrowanie danych oraz analizę wyników w czasie rzeczywistym. Raport statyczny przedstawia dane w ustalonej formie i nie umożliwia interakcji użytkownika.

### Zapytanie ad-hoc a wskaźnik KPI

Zapytanie ad-hoc jest tworzone jednorazowo w celu uzyskania odpowiedzi na konkretne pytanie analityczne. Wskaźnik KPI jest zdefiniowaną miarą biznesową monitorowaną regularnie w celu oceny wyników organizacji.

