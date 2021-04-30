.intel_syntax noprefix
.global sys_write
sys_write:
	mov rax, 1
	syscall
	ret

.global sys_exit
sys_exit:
	mov rax, 60
	syscall
	
