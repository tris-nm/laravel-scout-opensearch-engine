# Laravel Scout OpenSearch Engine
## Installation

`composer require tris-nm/laravel-scout-opensearch-engine`

Update your `App\Providers\AppServiceProvider`
```php
<?php

namespace App\Providers;

// ...

use Trisnm\LaravelScoutOpenSearchEngine\OpenSearchEngine;
use Laravel\Scout\EngineManager;

class AppServiceProvider extends ServiceProvider
{
    public function boot()
    {
        // ...

        resolve(EngineManager::class)->extend(config('scout.driver'), function () {
            return new OpenSearchEngine;
        });

    }
}
```

Update `config\scout.php`
```php
<?php

return [
    //
    'driver' => env('SCOUT_DRIVER', 'opensearch'),

    'opensearch' => [
        'host' => env('OPENSEACH_HOST', 'http://localhost:9200'),
        'user' => env('OPENSEACH_USER', 'admin'),
        'pass' => env('OPENSEACH_PASS', 'admin'),
        // Set value of basic auth is `false` incase aws Opensearch cluster use Access policy for security configuration
        'basic_auth' => env('OPENSEACH_BASIC_AUTH', true),
    ],
    //
];
```

## Usage
Learn more about [Laravel Scout](https://laravel.com/docs/9.x/scout)
