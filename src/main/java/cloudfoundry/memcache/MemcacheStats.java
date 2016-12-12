package cloudfoundry.memcache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.springframework.stereotype.Component;

@Component
public class MemcacheStats {
	
	public MemcacheStats() {
		Map<Byte, LongAdder> mutableOpcodeHits = new HashMap<>();
		for(MemcacheOpcodes memcacheOpcodes : MemcacheOpcodes.values()) {
			mutableOpcodeHits.put(memcacheOpcodes.opcode(), new LongAdder());
		}
		opcodeHits = Collections.unmodifiableMap(mutableOpcodeHits);
	}

	private final Map<Byte, LongAdder> opcodeHits;
	
	public void logHit(Byte opcode) {
		LongAdder value = opcodeHits.get(opcode);
		if(value == null) {
			value = opcodeHits.get(MemcacheOpcodes.UNKNOWN.opcode);
		}
		value.increment();
	}
	
	public Map<String, Long> getHitStats() {
		Map<String, Long> hitStats = new HashMap<>();
		for(MemcacheOpcodes memcacheOpcode : MemcacheOpcodes.values()) {
			hitStats.put(memcacheOpcode.name().toLowerCase(), opcodeHits.get(memcacheOpcode.opcode()).longValue());
		}
		return hitStats;
	}
	
	private enum MemcacheOpcodes {
	    GET((byte)0x00),
	    SET((byte)0x01),
	    ADD((byte)0x02),
	    REPLACE((byte)0x03),
	    DELETE((byte)0x04),
	    INCREMENT((byte)0x05),
	    DECREMENT((byte)0x06),
	    QUIT((byte)0x07),
	    FLUSH((byte)0x08),
	    GETQ((byte)0x09),
	    NOOP((byte)0x0a),
	    VERSION((byte)0x0b),
	    GETK((byte)0x0c),
	    GETKQ((byte)0x0d),
	    APPEND((byte)0x0e),
	    PREPEND((byte)0x0f),
	    STAT((byte)0x10),
	    SETQ((byte)0x11),
	    ADDQ((byte)0x12),
	    REPLACEQ((byte)0x13),
	    DELETEQ((byte)0x14),
	    INCREMENTQ((byte)0x15),
	    DECREMENTQ((byte)0x16),
	    QUITQ((byte)0x17),
	    FLUSHQ((byte)0x18),
	    APPENDQ((byte)0x19),
	    PREPENDQ((byte)0x1a),
	    TOUCH((byte)0x1c),
	    GAT((byte)0x1d),
	    GATQ((byte)0x1e),
	    GATK((byte)0x23),
	    GATKQ((byte)0x24),
	    SASL_LIST_MECHS((byte)0x20),
	    SASL_AUTH((byte)0x21),
	    SASL_STEP((byte)0x22),
	    UNKNOWN(null);
		
		private final Byte opcode;
		
	    MemcacheOpcodes(Byte opcode) {
	    	this.opcode = opcode;
	    }
	    
	    public Byte opcode() {
	    	return opcode;
	    }
	    
	    public static MemcacheOpcodes findByOpcode(Byte opcode) {
	    	for(MemcacheOpcodes memcacheOpcode : MemcacheOpcodes.values()) {
	    		if(memcacheOpcode.opcode() == opcode) {
	    			return memcacheOpcode;
	    		}
	    	}
	    	return MemcacheOpcodes.UNKNOWN;
	    }
	}
}