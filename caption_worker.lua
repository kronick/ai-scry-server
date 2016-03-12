-- Paranoid pirate worker protocol code based on https://github.com/moteus/lzmq-zguide/blob/master/examples/Lua-lzmq/ppworker.lua

-- zeromq for message passing
require "zhelpers"

require("luacurl")
local zmq = require "lzmq"
local ztimer = require "lzmq.timer"
local zloop = require "lzmq.loop"

local HEARTBEAT_LIVENESS = 10
local HEARTBEAT_INTERVAL = 1000
local INTERVAL_INIT = 1000
local INTERVAL_MAX = 32000

-- Paranoid Pirate Protocol constants
local PPP_READY     = "\001" -- Signals worker is ready
local PPP_HEARTBEAT = "\002" -- Signals worker heartbeat

local ZMQ_QUEUE_ADDRESS = "tcp://u1f52e.net:5560"

-- 
require 'torch'
require 'nn'
require 'nngraph'
-- exotics
-- local imports
local utils = require 'misc.utils'
require 'misc.DataLoader'
require 'misc.DataLoaderRaw'
require 'misc.LanguageModel'
local net_utils = require 'misc.net_utils'

local cv = require 'cv'
require 'cv.highgui'
require 'cv.videoio'
require 'cv.imgcodecs'
require 'cv.imgproc'

-------------------------------------------------------------------------------
-- Input arguments and options
-------------------------------------------------------------------------------
cmd = torch.CmdLine()
cmd:text()
cmd:text('Train an Image Captioning model')
cmd:text()
cmd:text('Options')

-- Input paths
cmd:option('-model','','path to model to evaluate')
cmd:option('-file', '', 'Which video file to process')
-- Basic options
cmd:option('-batch_size', 1, 'if > 0 then overrule, otherwise load from checkpoint.')
cmd:option('-num_images', 100, 'how many images to use when periodically evaluating the loss? (-1 = all)')
cmd:option('-language_eval', 0, 'Evaluate language as well (1 = yes, 0 = no)? BLEU/CIDEr/METEOR/ROUGE_L? requires coco-caption code from Github.')
cmd:option('-dump_images', 1, 'Dump images into vis/imgs folder for vis? (1=yes,0=no)')
cmd:option('-dump_json', 1, 'Dump json with predictions into vis folder? (1=yes,0=no)')
cmd:option('-dump_path', 0, 'Write image paths along with predictions into vis json? (1=yes,0=no)')
-- Sampling options
cmd:option('-sample_max', 1, '1 = sample argmax words. 0 = sample from distributions.')
cmd:option('-beam_size', 2, 'used when sample_max = 1, indicates number of beams in beam search. Usually 2 or 3 works well. More is not better. Set this to 1 for faster runtime but a bit worse performance.')
cmd:option('-temperature', 1.0, 'temperature when sampling from distributions (i.e. when sample_max = 0). Lower = "safer" predictions.')
-- misc
cmd:option('-backend', 'cudnn', 'nn|cudnn')
cmd:option('-id', 'evalscript', 'an id identifying this run/job. used only if language_eval = 1 for appending to intermediate files')
cmd:option('-seed', 123, 'random number generator seed to use')
cmd:option('-gpuid', 0, 'which gpu to use. -1 = use CPU')
cmd:text()

-------------------------------------------------------------------------------
-- Basic Torch initializations
-------------------------------------------------------------------------------
local opt = cmd:parse(arg)
torch.manualSeed(opt.seed)
torch.setdefaulttensortype('torch.FloatTensor') -- for CPU

if opt.gpuid >= 0 then
  require 'cutorch'
  require 'cunn'
  if opt.backend == 'cudnn' then require 'cudnn' end
  cutorch.manualSeed(opt.seed)
  cutorch.setDevice(opt.gpuid + 1) -- note +1 because lua is 1-indexed
end


-------------------------------------------------------------------------------
-- Load the model checkpoint to evaluate
-------------------------------------------------------------------------------
assert(string.len(opt.model) > 0, 'must provide a model')
local checkpoint = torch.load(opt.model)
-- override and collect parameters
if opt.batch_size == 0 then opt.batch_size = checkpoint.opt.batch_size end
local fetch = {'rnn_size', 'input_encoding_size', 'drop_prob_lm', 'cnn_proto', 'cnn_model', 'seq_per_img'}
for k,v in pairs(fetch) do 
  opt[v] = checkpoint.opt[v] -- copy over options from model
end
local vocab = checkpoint.vocab -- ix -> word mapping

-------------------------------------------------------------------------------
-- Load the networks from model checkpoint
-------------------------------------------------------------------------------
local protos = checkpoint.protos
protos.expander = nn.FeatExpander(opt.seq_per_img)
protos.lm:createClones() -- reconstruct clones inside the language model
if opt.gpuid >= 0 then for k,v in pairs(protos) do v:cuda() end end


-------------------------------------------------------------------------------
-- Get the public hostname of this instance for identification
-------------------------------------------------------------------------------
c = assert(curl.new())
local hostname = ""
assert(c:setopt(curl.OPT_URL, "http://169.254.169.254/latest/meta-data/public-hostname"))
assert(c:setopt(curl.OPT_WRITEDATA, output))
assert(c:setopt(curl.OPT_WRITEFUNCTION,function(stream, buffer)
                hostname = buffer
                return string.len(buffer);
        end));
assert(c:perform())

-------------------------------------------------------------------------------
-- Initialize ZeroMQ to receive filenames as messages
-------------------------------------------------------------------------------
-- Helper function that returns a new configured socket
-- connected to the Paranoid Pirate queue

identity = math.random(2^32)
function s_worker_socket(ctx)
  --identity = string.format("%04X-%04X", randof (0x10000), randof (0x10000))
  
  local worker, err = ctx:socket{zmq.DEALER,
    linger  = 0;
    connect = ZMQ_QUEUE_ADDRESS;
  }
  
  ctx:set(zmq.IDENTITY, identity)
  
  zassert(worker, err)
  print("Worker online!")
  print("Hostname is: " .. hostname)
  print("Identity is: " .. identity)
  worker:send_all({PPP_READY, hostname})
  return worker;
end

local ctx = zmq.context()
local worker = s_worker_socket(ctx)
local interval = INTERVAL_INIT

local loop = zloop.new(1, ctx)



-------------------------------------------------------------------------------
-- Evaluation fun(ction)
-------------------------------------------------------------------------------

local function run()
    protos.cnn:evaluate()
    protos.lm:evaluate()

    local reconnect_event
    local cycles   = 0
    
    
    -- This worker function runs in an endless loop, waiting for messages when a queue is available
    function worker_cb(worker)
        print("Waiting for new messages...")
        local msg = worker:recv_all()
        if not msg then return loop:interrupt() end
    
        if (#msg == 1) and (msg[1] == PPP_HEARTBEAT) then
            -- When we get a heartbeat message from the queue, it means the
            -- queue was (recently) alive, so we must reset our liveness
            -- indicator:
            print("Heartbeat received from queue!")
            reconnect_event:restart()
            
        else
            -- Any other message is one we should try to process
            -- Multipart message has 3 frames: [1] address [2] empty [3] payload
            local path = string.match(msg[3], "[^ ]+")
            local temp = string.match(msg[3], " ([^ ]+)")
            local n    = string.match(msg[3], "[0-9]+$")
            if n == "" then n = 10 end  -- Default to 10 matches if nothing given
            print("Processing file: '" .. path .. "' with temperature=" .. temp .. " returning " .. n .. " matches")
            

            -- Download the file and save it to a temp file
            c = assert(curl.new());
            local filename="temp-" .. identity .. ".jpg"
            local file=assert(io.open(filename, "wb"));
            assert(c:setopt(curl.OPT_WRITEFUNCTION, function (stream, buffer)
                    stream:write(buffer)
                    return string.len(buffer);
            end));
            assert(c:setopt(curl.OPT_WRITEDATA, file));
            assert(c:setopt(curl.OPT_HTTPHEADER, "Connection: Keep-Alive", "Accept-Language: en-us"));
            assert(c:setopt(curl.OPT_URL, path));
            assert(c:setopt(curl.OPT_CONNECTTIMEOUT, 15));
            assert(c:perform());
            file:close()


            local frame = cv.imread{filename=filename}
              
            local w = frame:size(2)
            local h = frame:size(1)
            
            -- take a central crop
            local crop = cv.getRectSubPix{image=frame, patchSize={h,h}, center={w/2, h/2}}
            local cropsc = cv.resize{src=crop, dsize={256,256}}
            -- BGR2RGB
            cropsc = cropsc:index(3,torch.LongTensor{3,2,1})
            -- HWC2CHW
            cropsc = cropsc:permute(3,1,2)
            
            -- fetch a batch of data
            local batch = cropsc:contiguous():view(1,3,256,256)
            local batch_processed = net_utils.prepro(batch, false, opt.gpuid >= 0) -- preprocess in place, and don't augment
            
            -- forward the model to get loss
            local feats = protos.cnn:forward(batch_processed)
            --print(feats)
            
            -- forward the model to also get generated samples for each image
            local sample_opts = { sample_max = opt.sample_max, beam_size = opt.beam_size, temperature = temp }
            local out = ""
            for i=1,n do
              local sequence = protos.lm:sample(feats, sample_opts)
              local sentence = net_utils.decode_sequence(vocab, sequence)
              out = out .. sentence[1] .. "\n"
              print(sentence[1])
            end
            
            --zmq.assert(zmq_socket:send(out))         
            worker:send_all({msg[1], msg[2], out})
            
            collectgarbage("collect")
        end
    end
    
    -- If the queue hasn't sent us heartbeats in a while, destroy the
    -- socket and reconnect. This is the simplest most brutal way of
    -- discarding any messages we might have sent in the meantime://
    function reconnect_cb(ev)
        printf ("W: heartbeat failure, can't reach queue\n");
        printf ("W: reconnecting in %d msec...\n", interval);
    
        loop:remove_socket(worker)
        worker:close()
        worker = nil
        ev:reset()
    
        loop:add_once(interval, function()
            -- Exponentially increase retry time up to a max
            if interval < INTERVAL_MAX then
                interval = interval * 2
            end
            worker = s_worker_socket(ctx)
            loop:add_socket(worker, worker_cb)
            
            
            reconnect_event = loop:add_interval(
                HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL,
                reconnect_cb
            )
        end)
    end
    
    loop:add_socket(worker, worker_cb)
    reconnect_event = loop:add_interval(
        HEARTBEAT_LIVENESS * HEARTBEAT_INTERVAL,
        reconnect_cb
    )
    loop:add_interval(HEARTBEAT_INTERVAL, function()
        -- Send heartbeat to queue if it's time
        if not worker then return end
        printf ("I: worker heartbeat\n");
        worker:send_all({PPP_HEARTBEAT, hostname})
    end)
    
    loop:start()
        
end


run()

--if pcall(run) then
--    print("Terminated without errors.")
--else
--    print("OH NO I DIED")
    --zmq_socket:close()
    --zmq_context:term()
--end