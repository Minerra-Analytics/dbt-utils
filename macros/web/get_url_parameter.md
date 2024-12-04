{% docs get_url_parameter %}

This macro extracts the value of a specified URL parameter from a URL string.

### Returns
* String value of the specified parameter, or NULL if the parameter is not found

### Example Usage

```sql
select
    {{ dbt_utils.get_url_parameter('url', 'utm_medium') }} as utm_medium
from {{ ref('data_urls') }}
```

### Integration Tests
Located in `integration_tests/`:
* Data: `data/web/data_urls.csv` - Sample URLs with UTM parameters
* Model: `models/web/test_urls.sql` - Tests parameter extraction
* Schema: `models/web/schema.yml` - Validates extracted values

### Test Cases
1. Single parameter extraction: `utm_medium=organic`
2. Multiple parameter handling: `utm_medium=organic&utm_source=github`
3. Missing parameter handling: Returns NULL
{% enddocs %}
