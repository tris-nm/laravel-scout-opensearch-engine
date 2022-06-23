<?php

namespace Trisnm\LaravelScoutOpenSearchEngine;

use Illuminate\Database\Eloquent\SoftDeletes;
use Illuminate\Support\LazyCollection;
use Illuminate\Support\Facades\Http;
use Illuminate\Http\Client\Response;
use Laravel\Scout\Engines\Engine;
use Laravel\Scout\Builder;
use Exception;
use Arr;

class OpenSearchEngine extends Engine
{
    /**
     * Determines if soft deletes for Scout are enabled or not.
     *
     * @var bool
     */
    protected $softDelete;

    /**
     * Opensearch cluster url
     */
    protected $url;

    /**
     * Determines if Opensearch need basic auth with username/pass
     */
    protected $useBasicAuth;

    /**
     * Create a new engine instance.
     *
     * @param  bool  $softDelete
     * @return void
     */
    public function __construct($softDelete = false)
    {
        $this->softDelete = $softDelete;
        $this->url = config('scout.opensearch.host');
        $this->useBasicAuth = config('scout.opensearch.basic_auth', true);
    }

    private function makeRequestToCluster($method, $path, $data = [])
    {
        if ($this->useBasicAuth) {
            return Http::withBasicAuth(config('scout.opensearch.user'), config('scout.opensearch.pass'))
                ->$method($path, $data);
        }

        return Http::$method($path, $data);
    }

    /**
     * Update the given model in the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     * @return void
     */
    public function update($models)
    {
        if ($models->isEmpty()) {
            return;
        }

        $index = $models->first()->searchableAs();

        if (in_array(SoftDeletes::class, class_uses_recursive($models->first())) && $this->softDelete) {
            $models->each->pushSoftDeleteMetadata();
        }

        $objects = $models->map(function ($model) {
            if (empty($searchableData = $model->toSearchableArray())) {
                return;
            }
            return array_merge(
                [
                    'id' => $model->getScoutKey()
                ],
                $searchableData
            );
        })->filter()->values()->all();

        if (!empty($objects)) {
            foreach ($objects as $object) {
                $path = $this->url . '/' . $index . '/_doc/' . $object['id'];
                $this->makeRequestToCluster('post', $path, $object);
            }
        }
    }

    /**
     * Remove the given model from the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     * @return void
     */
    public function delete($models)
    {

        $index = $models->first()->searchableAs();

        $ids = $models->map(function ($model) {
            return $model->getScoutKey();
        })->values()->all();

        foreach ($ids as $id) {
            $path = $this->url . '/' . $index . '/_doc/' . $id;
            $this->makeRequestToCluster('delete', $path);
        }
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [], bool $skipCallback = false)
    {
        if (count($builder->orders) > 0) {
            foreach ($builder->orders as $order) {
                $options['sort'][] = [
                    $order['column'] => [
                        'order' => $order['direction']
                    ]
                ];
            }
        }

        if ($builder->callback && !$skipCallback) {
            return call_user_func(
                $builder->callback,
                $this,
                $builder,
                $options
            );
        }

        $path = $this->url . '/' . $builder->model->searchableAs() . '/_search';
        $response = $this->makeRequestToCluster('post', $path, $options);
        self::errors($response);

        return $response->json('hits');
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        $options = [
            '_source' => true,
            'size' => $builder->limit ? $builder->limit : 10000,
            'from' => 0,
        ];
        $options['query'] = $this->filters($builder);

        return $this->performSearch($builder, $options);
    }

    public function searchRaw(Builder $builder, array $options)
    {
        return $this->performSearch($builder, $options, true);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $limit, $page)
    {
        return $this->performSearch($builder, array_filter([
            '_source' => true,
            'query' => $this->filters($builder),
            'size' =>  $limit ? $limit : 10,
            'from' => ($page - 1) * $limit
        ]));
    }

    /**
     * Get the filter array for the query.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        $query = null;
        if ($builder->query) {
            $fields = $builder->model->searchableFields();
            $query = [
                'bool' => [
                    'must' => [
                        [
                            'simple_query_string' => [
                                'query' => $builder->query,
                                'fields' => $fields,
                                'default_operator' => 'or',
                            ]
                        ]
                    ]
                ]
            ];
        }

        if (count($builder->wheres) > 0) {
            if (!isset($query)) $query = [];
            if (!isset($query['bool'])) $query['bool'] = [];
            foreach ($builder->wheres as $key => $value) {
                if ($key && $value) {
                    $query['bool']['filter'][] = [
                        'match' => [
                            $key => [
                                'query' => $value,
                                'operator' => 'and'
                            ]
                        ]
                    ];
                }
            }
        }

        return $query;
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function map(Builder $builder, $results, $model)
    {
        if (!is_array($results) || count($results['hits']) === 0) {
            return $model->newCollection();
        }

        $ids = $this->mapIds($results);
        $positions = array_flip($ids->toArray());

        return $model->getScoutModelsByIds($builder, $ids->toArray())->sortBy(function ($model) use ($positions) {
            return $positions[$model->getScoutKey()];
        })->values();
    }

    /**
     * Map the given results to instances of the given model via a lazy collection.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return \Illuminate\Support\LazyCollection
     */
    public function lazyMap(Builder $builder, $results, $model)
    {
        if (count($results['hits']) === 0) {
            return LazyCollection::make($model->newCollection());
        }

        $ids = $this->mapIds($results);
        $objectIdPositions = array_flip($ids->toArray());

        return $model->queryScoutModelsByIds(
            $builder,
            $ids
        )->cursor()->sortBy(function ($model) use ($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        })->values();
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits'])->pluck('_id')->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return (int) Arr::get($results, 'total.value', count($results['hits']));
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function flush($model)
    {
        $index = $model->searchableAs();
        $this->deleteIndex($index);
    }

    /**
     * Create a search index.
     *
     * @param  string  $name
     * @param  array  $options
     * @return mixed
     *
     * @throws \Exception
     */
    public function createIndex($name, array $options = [])
    {
        throw new Exception('OpenSearch indexes are created automatically upon adding objects.');
    }

    /**
     * Delete a search index.
     *
     * @param  string  $name
     * @return mixed
     */
    public function deleteIndex($name)
    {
        $path = $this->url . '/' . $name;
        $response = $this->makeRequestToCluster('delete', $path);
        if ($response->status() != 200) {
            return throw new Exception($response->reason());
        }
    }

    public static function errors(Response $response)
    {
        if ($response->status() != 200) {
            $data = $response->json();
            $reason = Arr::has($data, 'error.reason') ? Arr::get($data, 'error.reason') : $response->getReasonPhrase();
            return throw new Exception($reason);
        }
    }
}
