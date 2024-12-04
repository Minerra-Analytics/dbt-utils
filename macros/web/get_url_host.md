{% docs get_url_host %}
This macro extracts the host (domain) from a URL string.

get_url_host ([source](get_url_host.sql))


### Example Usage

```sql
select
    {{ dbt_utils.get_url_host('url') }} as host
from {{ ref('data_urls') }}
```

### Integration Tests
Located in `integration_tests/`:
* Data: `data/web/data_urls.csv` - Sample URLs with various domain formats
* Model: `models/web/test_url_host.sql` - Tests domain extraction
* Schema: `models/web/schema.yml` - Validates extracted domains

### Test Cases
1. HTTP URLs: `http://example.com/path`
2. HTTPS URLs: `https://example.com/path`
3. Android app URLs: `android-app://example.com/path`
4. URLs with query parameters: `example.com/path?param=value`

{% enddocs %}
