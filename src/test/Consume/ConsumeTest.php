<?php

namespace Ssdk\Rabbittest\Consume;

class ConsumeTest
{
    private $params;
    
    public function __construct($params)
    {
        $this->params = $params;
    }
    
    public function handle()
    {
        // TODO your logic
        
        echo 88;
        
        return true;
    }
}