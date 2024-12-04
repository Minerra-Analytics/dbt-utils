{% docs get_url_path %}

Extracts the path from a URL string.

### Usage
```sql
select
    {{ dbt_utils.get_url_path('url') }} as path
from {{ ref('data_urls') }}
```

### Returns
Returns the path portion of the URL.

### Examples

```sql
-- Example 1: Extracts the path from a URL
select
    {{ dbt_utils.get_url_path('url') }} as path
from {{ ref('data_urls') }}
```


{% enddocs %}
