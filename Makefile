TARGET_BIN=relly.bin
default: $(TARGET_BIN)

include ../../common.mk

.FORCE : 
$(TARGET_BIN) : .FORCE
	export LLVM_CC="$(LLVM_CC)" && \
		export LLVM_AR="$(LLVM_AR)" && \
		cargo build -vv --target=x86_64-unknown-elf.json --release \
		-Z build-std=core,alloc \
		-Z build-std-features=compiler-builtins-mem
	cp target/x86_64-unknown-elf/release/relly ./relly.bin
install: $(TARGET_BIN)
clean:
	-rm -r target
dump: $(TARGET_BIN)
	objdump -d target/x86_64-unknown-elf/release/relly
test:
	make
