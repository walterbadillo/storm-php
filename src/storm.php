<?php

/*
 * Storm Interfaces
 */

interface Storm_Bolt {
    public function run();
}
interface Storm_Spout {}

interface Storm_Component {
    public function retrieve();
    public function send($message);
    public function sendCommand(Storm_Command $command);
}

interface Storm_Shell_Input {
    public function retrieve();    
    public function read();
}
interface Storm_Shell_Output {
    public function send($message);
}

interface Storm_Serializer {
    public function encode($message);
    public function decode($encodedMessage);
}

class Storm_Tuple
{
    
    private $id;
    private $component;
    private $stream;
    private $task;
    private $values;

    public function __construct($id, $component, $stream, $task, $values)
    {
        $this->id           = $id;
        $this->component    = $component;
        $this->stream       = $stream;
        $this->task         = $task;
        $this->values       = $values;
    }
    
    public function getId()
    {
        return $this->id;
    }
    
    public function getComponent()
    {
        return $this->component;
    }
    
    public function getStream()
    {
        return $this->stream;
    }
    
    public function getTask()
    {
        return $this->task;
    }
    
    public function getValues()
    {
        return $this->values;
    }
    
    public function setValues($values) 
    {
        $this->values = $values;
    }
    
    public function __toArray()
    {
        return array(
            'id'        => $this->getId(),
            'component' => $this->getComponent(),
            'stream'    => $this->getStream(),
            'task'      => $this->getTask(),
            'tuple'     => $this->getValues()
        );
    }
    
    public function __toString()
    {
        return json_encode($this->__toArray());
    }
    
}

class Storm_Command {
    
    const EMIT = 'emit';
    const ACK  = 'ack';
    const FAIL = 'fail';
    
    private $type = null;
    private $data = array();
    
    public function __construct($type, array $data = array()) {
        $this->type = $type;
        $this->data = $data;
    }
    
    public function type() {
        return $this->type;
    }
    
    public function __toArray()
    {
        return array_merge(array(
            'command' => $this->type()
        ), $this->data);
    }
    
    public function __toString()
    {
        return json_encode($this->__toArray());
    }
    
}

abstract class Storm_Shell_Input_Abstract
    implements Storm_Shell_Input 
{
    
    /**
     *
     * @var Storm_Serializer
     */
    private $serialiser = null;
    
    public function __construct(Storm_Serializer $serialiser)
    {
        $this->serialiser = $serialiser;
    }
    
    public function retrieve()
    {
        $encodedMessage = $this->waitForMessage();
        $decodedMessage = $this->serialiser->decode($encodedMessage);
        
        return $decodedMessage;
    }
    
    /**
     * Read an entire message from stdin across multiple lines until a line has
     * a single token of 'end'
     * @return string
     */
    private function waitForMessage()
    {
        $message = '';
        
        while (true) {
            
            $line = trim($this->read());
            
            if (strlen($line) == 0) {
                continue;
            } else if ($line == 'end') {
                break;
            } else if ($line == 'sync') {
                $message = '';
                continue;
            }

            $message .= $line . "\n";
            
        }

        return trim($message);
    }
    
}

class Storm_Shell_Input_Stdin 
    extends Storm_Shell_Input_Abstract
{
    
    private $resource = null;
    
    public function __construct(Storm_Serializer $serialiser, $resource = null)
    {
        parent::__construct($serialiser);
        
        if (is_string($resource) && !empty($resource)) {
            $this->resource = fopen($resource, 'r');
        } else {
            $this->resource = STDIN;
        }
    }
    
    public function read()
    {
        return trim(fgets($this->resource));
    }
    
}

abstract class Storm_Shell_Output_Abstract
    implements Storm_Shell_Output 
{
    
    /**
     *
     * @var Storm_Serializer
     */
    private $serialiser = null;
    
    public function __construct(Storm_Serializer $serialiser)
    {
        $this->serialiser = $serialiser;
    }
    
    /**
     * Print a message to stdout; encode and echo a command (denoted with an
     * array) with an end marker
     * 
     * @param array|string $message
     */
    public function send($message) 
    {
        
        // case: Storm command
        if (is_array($message)) {
            
            $encodedMessage = $this->serialiser->encode($message);
            
            $this->output($encodedMessage);
            $this->output('end');
            
        } 
        
        else {
            $this->output($message);
        }
        
    }
    
    abstract protected function output($message);
    
}

class Storm_Shell_Output_Stdout
    extends Storm_Shell_Output_Abstract 
{
    
    private $resource = null;
    
    public function __construct(Storm_Serializer $serialiser, $resource = null)
    {
        parent::__construct($serialiser);
        
        if (is_string($resource) && !empty($resource)) {
            $this->resource = fopen($resource, 'w');
        } else {
            $this->resource = STDOUT;
        }
    }
    
    protected function output($message) {
        fwrite($this->resource, (string) $message . "\n");
    }
    
}

class Storm_Shell_Output_Decorator_LogFile implements Storm_Shell_Output {
    
    private $logFile = null;
    private $logResource = null;
    private $decoratedShellOutput = null;
    
    public function __construct(Storm_Shell_Output $shellOutput, $logFile) 
    {
        $this->decoratedShellOutput = $shellOutput;
        $this->initialiseLog($logFile);
    }
    
    private function initialiseLog($logFile) 
    {
        $this->logFile = $logFile;
        $this->logResource = fopen($logFile, 'a');
    }
    
    public function send($message) {
        $this->log($message);
        $this->decoratedShellOutput->send($message);
    }
    
    private function log($message) 
    {
        if ($this->logResource) {
            
            if (is_array($message) || is_bool($message)) {
                $message = json_encode($message);
            }
            
            $log = date('Y-m-d\TH:i:s') . ' OUTPUT: ' . $message . "\n";
            fwrite($this->logResource, $log);
        }
    }
    
}

class Storm_Shell_Input_Decorator_LogFile implements Storm_Shell_Input {
    
    private $decoratedShellInput = null;
    private $logFile = null;
    private $logResource = null;
    
    public function __construct(Storm_Shell_Input $shellReader, $logFile) 
    {
        $this->decoratedShellInput = $shellReader;
        $this->initialiseLog($logFile);
    }
    
    private function initialiseLog($logFile) 
    {
        $this->logFile = $logFile;
        $this->logResource = fopen($logFile, 'a');
    }
    
    public function retrieve()
    {
        $message = $this->decoratedShellInput->retrieve();
        $this->log($message);
        return $message;
    }
    
    public function read()
    {
        return $this->decoratedShellInput->read();
    }
    
    private function log($message) 
    {
        if ($this->logResource) {
            
            if (is_array($message) || is_bool($message)) {
                $message = json_encode($message);
            }
            
            $log = date('Y-m-d\TH:i:s') . '  INPUT: ' . $message . "\n";
            fwrite($this->logResource, $log);
        }
    }
    
}


class Storm_Serializer_Json implements Storm_Serializer {
    
    public function encode($message)
    {
        $encodedMessage = json_encode($message);
        return $encodedMessage;
    }
    
    public function decode($encodedMessage)
    {
        $decodedMessage = json_decode($encodedMessage, true);
        return ($decodedMessage) ? $decodedMessage : $encodedMessage;
    }
    
}

class Storm_Serializer_32BitJson implements Storm_Serializer {
    
    public function encode($message)
    {
        $encodedMessage = json_encode($message);
        
        // convert string-based IDs into long integers to prevent casting errors
        // in the Java client
        $encodedMessage = preg_replace('/"id":"([-\d]+)"/mi', '"id":$1', $encodedMessage);
        $encodedMessage = preg_replace('/"anchors":\["([-\d]+)"\]/mi', '"anchors":[$1]', $encodedMessage);
        
        return $encodedMessage;
    }
    
    public function decode($encodedMessage)
    {
        // prevent loss of ID due to long precision
        $encodedMessage = preg_replace('/"id":([-\d]+)/mi', '"id":"$1"', $encodedMessage);
        
        $decodedMessage = json_decode($encodedMessage, true);
        
        return ($decodedMessage) ? $decodedMessage : $encodedMessage;
    }
    
}

class Storm_Component_Shell
    implements Storm_Component
{
    protected $pid;
    protected $configuration;
    protected $topologyContext;
    
    private $input;
    private $output;
    
    public function __construct(Storm_Shell_Input $input, Storm_Shell_Output $output)
    {
        
        $this->pid = getmypid();
        
        $this->input = $input;
        $this->output = $output;
        
    }
    
    public function initialise() 
    {
        $this->send($this->pid);

        $pidDir = $this->input->read();
        @fclose(@fopen($pidDir . "/" . $this->pid, "w"));

        $this->configuration = $this->retrieve();
        $this->topologyContext = $this->retrieve();
    }
    
    public function getConfiguration()
    {
        return $this->configuration;
    }
    
    public function getTopologyContext()
    {
        return $this->topologyContext;
    }
    
    public function retrieve() {
        return $this->input->retrieve();
    }
    
    public function send($message) {        
        return $this->output->send($message);
    }
    
    public function sendCommand(Storm_Command $command)
    {
        $message = $command->__toArray();
        $this->output->send($message);
    }

    /*protected function sendLog($message)
    {
        return $this->sendCommand(array(
            'command' => 'log',
            'msg' => $message
        ));
    }*/

}

abstract class Storm_Shell_Bolt
    implements Storm_Bolt 
{

    protected $runnable = false;
    
    /**
     *
     * @var Storm_Component
     */
    private $component = null;
    
    private $anchorTuple = null;

    public function __construct(Storm_Component $component)
    {
        $this->component = $component;
    }
    
    protected function getComponent()
    {
        return $this->component;
    }
    
    public function getConfiguration()
    {
        return $this->getComponent()->getConfiguration();
    }
    
    public function getTopologyContext()
    {
        return $this->getComponent()->getTopologyContext();
    }

    public function run()
    {
        try {
    
            $this->initialise();
            
            while($this->runnable) {
   
                $command = $this->getComponent()->retrieve();

                if (is_array($command) && isset($command['tuple'])) {
    
                    $tuple = array_merge(array(
                        'id'        => null,
                        'comp'      => null,
                        'stream'    => null,
                        'task'      => null,
                        'tuple'     => null
                    ), $command);

                    $tuple = new Storm_Tuple(
                        $tuple['id'], 
                        $tuple['comp'], 
                        $tuple['stream'], 
                        $tuple['task'], 
                        $tuple['tuple']
                    );

                    $this->process($tuple);

                    $this->sync();

                }
                
            }
            
        } catch(Exception $e) {
            echo $e->getTraceAsString();
        }
        
    }

    abstract protected function process(Storm_Tuple $tuple);

    protected function initialise() 
    {
        $this->runnable = true;
        $this->getComponent()->initialise();
    }

    final protected function sync()
    {
        $this->getComponent()->send('sync');
    }

    final protected function ack(Storm_Tuple $tuple)
    {
        $command = new Storm_Command(Storm_Command::ACK, array(
            'id' => $tuple->getId()
        ));
        
        $this->getComponent()->sendCommand($command);
    }

    final protected function fail(Storm_Tuple $tuple)
    {
        $command = new Storm_Command(Storm_Command::FAIL, array(
            'id' => $tuple->getId()
        ));
        
        $this->getComponent()->sendCommand($command);
    }
    
    final protected function emit(Storm_Tuple $tuple, $stream = null, $anchors = array())
    {
        $this->emitTuple($tuple, $stream, $anchors);
    }

    final protected function emitDirect($directTask, Storm_Tuple $tuple, $stream = null, $anchors = array())
    {
        $this->emitTuple($tuple, $stream, $anchors, $directTask);
    }
    
    private function emitTuple(Storm_Tuple $tuple, $stream = null, $anchors = array(), $directTask = null)
    {
    
        if ($this->anchorTuple !== null) {
            $anchors = array($this->anchorTuple);
        }

        $command = array();

        if($stream !== null) {
            $command['stream'] = $stream;
        }

        $command['anchors'] = array_map(function($a) {
            return $a->getId();
        }, $anchors);

        if($directTask !== null) {
            $command['task'] = $directTask;
        }

        $command['tuple'] = $tuple->getValues();
        
        $command = new Storm_Command(Storm_Command::EMIT, $command);
        $this->getComponent()->sendCommand($command);
        
    }
    
}

abstract class Storm_Basic_Bolt 
    extends Storm_Shell_Bolt
{
    
    public function run()
    {

        try {

            $this->initialise();
            
            while($this->runnable) {
 
                $command = $this->getComponent()->retrieve();

                if (is_array($command)) {

                    if (isset($command['tuple'])) {
                      
                        $tuple = array_merge(array(
                            'id'        => null,
                            'comp'      => null,
                            'stream'    => null,
                            'task'      => null,
                            'tuple'     => null
                        ), $command);

                        $tuple = new Storm_Tuple(
                            $tuple['id'], 
                            $tuple['comp'], 
                            $tuple['stream'], 
                            $tuple['task'], 
                            $tuple['tuple']
                        );
                        
                        $this->anchorTuple = $tuple;

                        try {

                            $processed = $this->process($tuple);

                            if ($processed) {
                                $this->ack($tuple);
                            } else {
                                $this->fail($tuple);
                            }

                        } catch (Exception $e) {
                            $this->fail($tuple);
                        }

                        $this->sync();

                    }
                }
            }
        } catch(Exception $e) {
            //$this->sendLog($e->getTraceAsSTring());
        }

    }
    
}
/*
abstract class StormShellSpout 
    extends StormShellComponent 
    implements StormSpout
{

    protected $tuples = array();

    public function __construct()
    {
        parent::__construct();

        $this->init($this->stormConf, $this->topologyContext);
    }

    abstract protected function nextTuple();
    abstract protected function ack($tupleId);
    abstract protected function fail($tupleId);

    public function run()
    {
    
        while (true) {
    
            
            $command = $this->parseMessage( $this->waitForMessage() );

            if (is_string($command) && $command == 'next') {
                $this->nextTuple();
            } else if (is_array($command)) {
    
                if (isset($command['command'])) {
                    
                    switch($command['command']) {
                        case 'ack':
                            $this->ack($command['id']);
                            break;
                        case 'fail':
                            $this->fail($command['id']);
                            break;
                    }
                    
                }
                
            }
        }
    }

    protected function init($stormConf, $topologyContext)
    {
    }

    final protected function emit(array $tuple, $messageId = null, $streamId = null)
    {
        return $this->emitTuple($tuple, $messageId, $streamId, null);
    }

    final protected function emitDirect($directTask, array $tuple, $messageId = null, $streamId = null)
    {
        return $this->emitTuple($tuple, $messageId, $streamId, $directTask);
    }

    final private function emitTuple(array $tuple, $messageId = null, $streamId = null, $directTask = null)
    {
        $command = array(
            'command' => 'emit'
        );

        if ($messageId !== null) {
            $command['id'] = $messageId;
        }

        if ($streamId !== null) {
            $command['stream'] = $streamId;
        }

        if ($directTask !== null) {
            $command['task'] = $directTask;
        }

        $command['tuple'] = $tuple;

        return $this->sendCommand($command);
    }
}
*/

class Storm_Component_Factory 
{
    
    private $serialiserFactory = null;
    private $inputFactory = null;
    private $outputFactory = null;
    
    public function __construct()
    {
        $this->serialiserFactory = new Storm_Serializer_Factory();
        $this->inputFactory = new Storm_Shell_Input_Factory();
        $this->outputFactory = new Storm_Shell_Output_Factory();
    }
    
    public function create(array $options = array())
    {
        
        $serialiser = $this->createSerialiser($options);
        
        $input = $this->createInput($serialiser, $options);
        $output = $this->createOutput($serialiser, $options);
        
        $component = new Storm_Component_Shell($input, $output);
        
        return $component;
    }
    
    protected function createSerialiser($options)
    {
        $serialiserType = isset($options['serialiser']) 
            ? $options['serialiser'] 
            : null;
        $serialiser = $this->serialiserFactory->create($serialiserType);
        
        return $serialiser;
    }
    
    protected function createInput($serialiser, $options)
    {
        $meta = isset($options['input']) 
            ? $options['input'] 
            : null;
        
        $type = null;
        $options = array();
        
        if (is_string($meta)) {
            $type = $meta;
        } else if (is_array($meta)) {
            $type = isset($meta['type']) ? $meta['type'] : null;
            $options = isset($meta['options']) ? (array) $meta['options'] : array();
        }
        
        $input = $this->inputFactory->create($type, $serialiser, $options);
        
        return $input;
    }
    
    protected function createOutput($serialiser, $options)
    {
        $meta = isset($options['output']) 
            ? $options['output'] 
            : null;
        
        $type = null;
        $options = array();
        
        if (is_string($meta)) {
            $type = $meta;
        } else if (is_array($meta)) {
            $type = isset($meta['type']) ? $meta['type'] : null;
            $options = isset($meta['options']) ? (array) $meta['options'] : array();
        }
        
        $output = $this->outputFactory->create($type, $serialiser, $options);
        
        return $output;
    }
    
}

class Storm_Serializer_Factory 
{
    public function create($type)
    {
        $serialiser = null;
        
        switch($type)
        {
            case '32bit':
                $serialiser = new Storm_Serializer_32BitJson();
                break;
            default:
                $serialiser = new Storm_Serializer_Json();
                break;
        }
        
        return $serialiser;
    }
}

class Storm_Shell_Input_Factory 
{
    
    public function create($type, Storm_Serializer $serialiser, array $options = array()) 
    {
        
        $input = null;
        
        switch($type)
        {
            case 'stdin':
            default:
                $input = new Storm_Shell_Input_Stdin($serialiser);
                break;
        }
        
        $input = $this->decorate($input, $options);
        
        return $input;
        
    }
    
    protected function decorate(Storm_Shell_Input $input, array $options = array()) 
    {
        $decorators = isset($options['decorators'])
            ? (array) $options['decorators']
            : array();
        
        foreach ($decorators as $decoratorType => $decoratorOptions) {
            
            switch($decoratorType) {
                case 'log-file':
                    
                    $logFile = isset($decoratorOptions['file'])
                        ? (string) $decoratorOptions['file']
                        : null;
                    
                    if (!empty($logFile)) {
                        $input = new Storm_Shell_Input_Decorator_LogFile($input, $logFile);
                    }
                    
                    break;
            }
            
        }
        
        return $input;
    }
    
}

class Storm_Shell_Output_Factory 
{
    
    public function create($type, Storm_Serializer $serialiser, array $options = array()) 
    {
        
        $output = null;
        
        switch($type)
        {
            case 'stdout':
            default:
                $output = new Storm_Shell_Output_Stdout($serialiser);
                break;
        }
        
        $output = $this->decorate($output, $options);
        
        return $output;
        
    }
    
    protected function decorate(Storm_Shell_Output $output, array $options = array()) 
    {
        $decorators = isset($options['decorators'])
            ? (array) $options['decorators']
            : array();
        
        foreach ($decorators as $decoratorType => $decoratorOptions) {
            
            switch($decoratorType) {
                case 'log-file':
                    
                    $logFile = isset($decoratorOptions['file'])
                        ? (string) $decoratorOptions['file']
                        : null;
                    
                    if (!empty($logFile)) {
                        $output = new Storm_Shell_Output_Decorator_LogFile($output, $logFile);
                    }
                    
                    break;
            }
            
        }
        
        return $output;
    }
    
}