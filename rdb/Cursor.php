<?php

namespace r;

use Iterator;
use r\Exceptions\RqlDriverError;
use r\ProtocolBuffer\ResponseResponseType;
use r\DatumConverter;
use Amp;

class Cursor {

	private $token;
	private $connection;
	private $notes;
	private $toNativeOptions;
	private $currentData;
	private $currentSize;
	private $currentIndex;
	private $isComplete;
	private $wasIterated;

	public function __construct(
		Connection $connection,
		$initialResponse,
		$token,
		$notes,
		$toNativeOptions
	) {
		$this->connection      = $connection;
		$this->token           = $token;
		$this->notes           = $notes;
		$this->toNativeOptions = $toNativeOptions;
		$this->wasIterated     = false;

		$this->setBatch( $initialResponse );
	}

	public function next() {
		return Amp\pipe( $this->requestMoreIfNecessary(),
			function ( $more ) {
				if ( ! $this->valid() ) {
					throw new RqlDriverError( "No more data available." );
				}

				return $this->currentData[ $this->currentIndex ++ ]->toNative( $this->toNativeOptions );
			} );
	}

	/**
	 * @param $callback
	 * @param null|callable $onFinishedCallback
	 */
	public function each( $callback, $onFinishedCallback = null ) {
		$deferred = new Amp\deferred();
		$each     = function () use ( $callback, $onFinishedCallback, &$each, $deferred ) {
			Amp\pipe( $this->requestMoreIfNecessary(),
				function ( $more ) use ( $callback, $onFinishedCallback, &$each, $deferred ) {
					$result = $callback(
						$this->currentData[ $this->currentIndex ++ ]->toNative( $this->toNativeOptions )
					);

					if ( $result !== false && ! $this->isComplete ) {
						Amp\immediately( $each );
					} else {
						$deferred->succeed();
					}

					if ( $this->isComplete ) {
						if ( $onFinishedCallback != null ) {
							$onFinishedCallback();
						}
						$deferred->succeed();
					}
				} );
		};

		Amp\immediately( $each );

		return $deferred->promise();
	}

	public function valid() {
		$this->requestMoreIfNecessary();

		return ! $this->isComplete || ( $this->currentIndex < $this->currentSize );
	}

	public function toArray() {
		$result = array();
		foreach ( $this as $val ) {
			$result[] = $val;
		}

		return $result;
	}

	public function close() {
		if ( ! $this->isComplete ) {
			// Cancel the request
			$this->connection->stopQuery( $this->token );
			$this->isComplete = true;
		}
		$this->currentIndex = 0;
		$this->currentSize  = 0;
		$this->currentData  = array();
	}

	public function bufferedCount() {
		$this->currentSize - $this->currentIndex;
	}

	public function getNotes() {
		return $this->notes;
	}

	public function __toString() {
		return "Cursor";
	}


	public function __destruct() {
		if ( $this->connection->isOpen() ) {
			// Cancel the request
			$this->close();
		}
	}

	private function requestMoreIfNecessary() {
		if ( $this->currentIndex == $this->currentSize ) {
			// We are at the end of currentData. Request more if available
			if ( $this->isComplete ) {
				return false;
			}

			return $this->requestNewBatch();
		}

		return false;
	}

	private function requestNewBatch() {
		try {
			return Amp\pipe( $this->connection->continueQuery( $this->token ),
				function ( $response ) {
					$this->setBatch( $response );

					return true;
				} );
		} catch ( \Exception $e ) {
			$this->isComplete = true;
			$this->close();
			throw $e;
		}
	}

	private function setBatch( $response ) {
		$dc                 = new DatumConverter;
		$this->isComplete   = $response['t'] == ResponseResponseType::PB_SUCCESS_SEQUENCE;
		$this->currentIndex = 0;
		$this->currentSize  = \count( $response['r'] );
		$this->currentData  = array();
		foreach ( $response['r'] as $row ) {
			$this->currentData[] = $datum = $dc->decodedJSONToDatum( $row );
		}
	}
}
